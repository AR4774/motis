#include "motis/paxmon/paxmon.h"

#include <algorithm>
#include <memory>
#include <numeric>
#include <set>

#include "boost/filesystem.hpp"

#include "fmt/format.h"

#include "utl/to_vec.h"
#include "utl/verify.h"
#include "utl/zip.h"

#include "motis/core/common/date_time_util.h"
#include "motis/core/common/logging.h"
#include "motis/core/journey/message_to_journeys.h"
#include "motis/module/context/get_schedule.h"
#include "motis/module/context/motis_call.h"
#include "motis/module/context/motis_publish.h"
#include "motis/module/message.h"

#include "motis/paxmon/broken_interchanges_report.h"
#include "motis/paxmon/build_graph.h"
#include "motis/paxmon/data_key.h"
#include "motis/paxmon/graph_access.h"
#include "motis/paxmon/loader/csv/csv_journeys.h"
#include "motis/paxmon/loader/journeys/motis_journeys.h"
#include "motis/paxmon/localization.h"
#include "motis/paxmon/messages.h"
#include "motis/paxmon/monitoring_event.h"
#include "motis/paxmon/output/journey_converter.h"
#include "motis/paxmon/over_capacity_report.h"
#include "motis/paxmon/reachability.h"
#include "motis/paxmon/update_load.h"

namespace fs = boost::filesystem;

using namespace motis::module;
using namespace motis::routing;
using namespace motis::logging;
using namespace motis::rt;

namespace motis::paxmon {

paxmon::paxmon() : module("Passenger Monitoring", "paxmon") {
  param(journey_files_, "journeys", "csv journeys or routing responses");
  param(capacity_files_, "capacity", "train capacities");
  param(stats_file_, "stats", "statistics file");
  param(capacity_match_log_file_, "capacity_match_log",
        "capacity match log file");
  param(journey_match_log_file_, "journey_match_log", "journey match log file");
  param(initial_over_capacity_report_file_, "over_capacity_report",
        "initial over capacity report file");
  param(initial_broken_report_file_, "broken_report",
        "initial broken interchanges report file");
  param(reroute_unmatched_, "reroute_unmatched", "reroute unmatched journeys");
  param(initial_reroute_query_file_, "reroute_file",
        "output file for initial rerouted journeys");
  param(initial_reroute_router_, "reroute_router",
        "router for initial reroute queries");
  param(start_time_, "start_time", "evaluation start time");
  param(end_time_, "end_time", "evaluation end time");
  param(time_step_, "time_step", "evaluation time step (seconds)");
  param(match_tolerance_, "match_tolerance",
        "journey match time tolerance (minutes)");
}

paxmon::~paxmon() = default;

void paxmon::init(motis::module::registry& reg) {
  LOG(info) << "paxmon module loaded";

  stats_writer_ = std::make_unique<stats_writer>(stats_file_);

  add_shared_data(DATA_KEY, &data_);

  reg.subscribe("/init", [&]() {
    load_capacity_files();
    load_journeys();
  });
  reg.register_op("/paxmon/flush", [&](msg_ptr const&) -> msg_ptr {
    stats_writer_->flush();
    return {};
  });
  reg.subscribe("/rt/update",
                [&](msg_ptr const& msg) { return rt_update(msg); });
  reg.subscribe("/rt/graph_updated", [&](msg_ptr const&) {
    scoped_timer t{"paxmon: graph_updated"};
    rt_updates_applied();
    return nullptr;
  });

  // --init /paxmon/eval
  // --paxmon.start_time YYYY-MM-DDTHH:mm
  // --paxmon.end_time YYYY-MM-DDTHH:mm
  reg.register_op(
      "/paxmon/eval",
      [&](msg_ptr const&) -> msg_ptr {
        auto const forward = [](std::time_t time) {
          using namespace motis::ris;
          message_creator fbb;
          fbb.create_and_finish(MsgContent_RISForwardTimeRequest,
                                CreateRISForwardTimeRequest(fbb, time).Union(),
                                "/ris/forward");
          LOG(info) << "paxmon: forwarding time to: " << format_unix_time(time);
          motis_call(make_msg(fbb))->val();
        };

        LOG(info) << "paxmon: start time: " << format_unix_time(start_time_)
                  << ", end time: " << format_unix_time(end_time_);

        for (auto t = start_time_; t <= end_time_; t += time_step_) {
          forward(t);
        }

        motis_call(make_no_msg("/paxmon/flush"))->val();

        return {};
      },
      ctx::access_t::WRITE);
}

void print_graph_stats(graph_statistics const& graph_stats) {
  LOG(info) << fmt::format("{:n} passenger groups, {:n} passengers",
                           graph_stats.passenger_groups_,
                           graph_stats.passengers_);
  LOG(info) << fmt::format("{:n} graph nodes ({:n} canceled)",
                           graph_stats.nodes_, graph_stats.canceled_nodes_);
  LOG(info) << fmt::format(
      "{:n} graph edges ({:n} canceled): {:n} trip + {:n} interchange + {:n} "
      "wait + {:n} through",
      graph_stats.edges_, graph_stats.canceled_edges_, graph_stats.trip_edges_,
      graph_stats.interchange_edges_, graph_stats.wait_edges_,
      graph_stats.through_edges_);
  LOG(info) << fmt::format("{:n} stations", graph_stats.stations_);
  LOG(info) << fmt::format("{:n} trips", graph_stats.trips_);
  LOG(info) << fmt::format("over capacity: {:n} trips, {:n} edges",
                           graph_stats.trips_over_capacity_,
                           graph_stats.edges_over_capacity_);
  LOG(info) << fmt::format("broken: {:n} interchange edges, {:n} groups",
                           graph_stats.broken_edges_,
                           graph_stats.broken_passenger_groups_);
}

loader::loader_result paxmon::load_journeys(std::string const& file) {
  auto const journey_path = fs::path{file};
  if (!fs::exists(journey_path)) {
    LOG(warn) << "journey file not found: " << file;
    return {};
  }
  auto const& sched = get_schedule();
  auto result = loader::loader_result{};
  if (journey_path.extension() == ".txt") {
    scoped_timer journey_timer{"load motis journeys"};
    result = loader::journeys::load_journeys(sched, data_, file);
  } else if (journey_path.extension() == ".csv") {
    scoped_timer journey_timer{"load csv journeys"};
    result = loader::csv::load_journeys(
        sched, data_, file, journey_match_log_file_, match_tolerance_);
  } else {
    LOG(logging::error) << "paxmon: unknown journey file type: " << file;
  }
  LOG(result.loaded_journeys_ != 0 ? info : warn)
      << "loaded " << result.loaded_journeys_ << " journeys from " << file;
  return result;
}

msg_ptr initial_reroute_query(schedule const& sched,
                              loader::unmatched_journey const& uj,
                              std::string const& router) {
  message_creator fbb;
  auto const planned_departure =
      motis_to_unixtime(sched.schedule_begin_, uj.departure_time_);
  auto const interval = Interval{planned_departure - 2 * 60 * 60,
                                 planned_departure + 2 * 60 * 60};
  auto const& start_station = sched.stations_.at(uj.start_station_idx_);
  auto const& destination_station =
      sched.stations_.at(uj.destination_station_idx_);
  fbb.create_and_finish(
      MsgContent_RoutingRequest,
      CreateRoutingRequest(
          fbb, Start_PretripStart,
          CreatePretripStart(
              fbb,
              CreateInputStation(fbb, fbb.CreateString(start_station->eva_nr_),
                                 fbb.CreateString(start_station->name_)),
              &interval)
              .Union(),
          CreateInputStation(fbb,
                             fbb.CreateString(destination_station->eva_nr_),
                             fbb.CreateString(destination_station->name_)),
          SearchType_Default, SearchDir_Forward,
          fbb.CreateVector(std::vector<flatbuffers::Offset<Via>>{}),
          fbb.CreateVector(
              std::vector<flatbuffers::Offset<AdditionalEdgeWrapper>>{}))
          .Union(),
      router);
  return make_msg(fbb);
}

void paxmon::load_journeys() {
  auto const& sched = get_schedule();

  if (journey_files_.empty()) {
    LOG(warn) << "paxmon: no journey files specified";
    return;
  }

  {
    std::unique_ptr<output::journey_converter> converter;
    if (reroute_unmatched_ && !initial_reroute_query_file_.empty()) {
      converter = std::make_unique<output::journey_converter>(
          initial_reroute_query_file_);
    }
    for (auto const& file : journey_files_) {
      auto const result = load_journeys(file);
      if (reroute_unmatched_) {
        scoped_timer timer{"reroute unmatched journeys"};
        LOG(info) << "routing " << result.unmatched_journeys_.size()
                  << " unmatched journeys using " << initial_reroute_router_
                  << "...";
        auto const futures =
            utl::to_vec(result.unmatched_journeys_, [&](auto const& uj) {
              return motis_call(
                  initial_reroute_query(sched, uj, initial_reroute_router_));
            });
        ctx::await_all(futures);
        LOG(info) << "adding replacement journeys...";
        for (auto const& [uj, fut] :
             utl::zip(result.unmatched_journeys_, futures)) {
          auto const rr_msg = fut->val();
          auto const rr = motis_content(RoutingResponse, rr_msg);
          auto const journeys = message_to_journeys(rr);
          if (journeys.empty()) {
            continue;
          }
          // TODO(pablo): select journey(s)
          if (converter) {
            converter->write_journey(journeys.front(), uj.source_.primary_ref_,
                                     uj.source_.secondary_ref_, uj.passengers_);
          }
          loader::journeys::load_journey(sched, data_, journeys.front(),
                                         uj.source_, uj.passengers_,
                                         group_source_flags::MATCH_REROUTED);
        }
      }
    }
  }

  build_graph_from_journeys(sched, data_);

  auto const graph_stats = calc_graph_statistics(sched, data_);
  print_graph_stats(graph_stats);
  if (graph_stats.trips_over_capacity_ > 0 &&
      !initial_over_capacity_report_file_.empty()) {
    write_over_capacity_report(data_, sched,
                               initial_over_capacity_report_file_);
  }
  if (!initial_broken_report_file_.empty()) {
    write_broken_interchanges_report(data_, initial_broken_report_file_);
  }
}

void paxmon::load_capacity_files() {
  auto const& sched = get_schedule();
  auto total_entries = 0ULL;
  for (auto const& file : capacity_files_) {
    auto const capacity_path = fs::path{file};
    if (!fs::exists(capacity_path)) {
      LOG(warn) << "capacity file not found: " << file;
      continue;
    }
    auto const entries_loaded =
        load_capacities(sched, file, data_.trip_capacity_map_,
                        data_.category_capacity_map_, capacity_match_log_file_);
    total_entries += entries_loaded;
    LOG(info) << fmt::format("loaded {:n} capacity entries from {}",
                             entries_loaded, file);
  }
  if (total_entries == 0) {
    LOG(warn)
        << "no capacity data loaded, all trips will have unknown capacity";
  }
}

void check_broken_interchanges(
    paxmon_data& data, schedule const& /*sched*/,
    std::vector<edge*> const& updated_interchange_edges,
    system_statistics& system_stats) {
  static std::set<edge*> broken_interchanges;
  static std::set<passenger_group*> affected_passenger_groups;
  for (auto& ice : updated_interchange_edges) {
    if (ice->type_ != edge_type::INTERCHANGE) {
      continue;
    }
    auto const from = ice->from(data.graph_);
    auto const to = ice->to(data.graph_);
    auto const ic = static_cast<int>(to->time_) - static_cast<int>(from->time_);
    if (ice->is_canceled(data.graph_) ||
        (from->station_ != 0 && to->station_ != 0 &&
         ic < ice->transfer_time())) {
      if (ice->broken_) {
        continue;
      }
      ice->broken_ = true;
      if (broken_interchanges.insert(ice).second) {
        ++system_stats.total_broken_interchanges_;
      }
      for (auto& psi : ice->pax_connection_info_.section_infos_) {
        if (affected_passenger_groups.insert(psi.group_).second) {
          system_stats.total_affected_passengers_ += psi.group_->passengers_;
          psi.group_->ok_ = false;
        }
        data.groups_affected_by_last_update_.insert(psi.group_);
      }
    } else {
      if (!ice->broken_) {
        continue;
      }
      ice->broken_ = false;
      // interchange valid again
      for (auto& psi : ice->pax_connection_info_.section_infos_) {
        data.groups_affected_by_last_update_.insert(psi.group_);
      }
    }
  }
}

msg_ptr paxmon::rt_update(msg_ptr const& msg) {
  auto const& sched = get_schedule();
  auto update = motis_content(RtUpdates, msg);

  tick_stats_.rt_updates_ += update->updates()->size();

  std::vector<edge*> updated_interchange_edges;
  for (auto const& u : *update->updates()) {
    switch (u->content_type()) {
      case Content_RtDelayUpdate: {
        ++system_stats_.delay_updates_;
        ++tick_stats_.rt_delay_updates_;
        auto const du = reinterpret_cast<RtDelayUpdate const*>(u->content());
        update_event_times(sched, data_.graph_, du, updated_interchange_edges,
                           system_stats_);
        tick_stats_.rt_delay_event_updates_ += du->events()->size();
        for (auto const& uei : *du->events()) {
          switch (uei->reason()) {
            case TimestampReason_IS: ++tick_stats_.rt_delay_is_updates_; break;
            case TimestampReason_FORECAST:
              ++tick_stats_.rt_delay_forecast_updates_;
              break;
            case TimestampReason_PROPAGATION:
              ++tick_stats_.rt_delay_propagation_updates_;
              break;
            case TimestampReason_REPAIR:
              ++tick_stats_.rt_delay_repair_updates_;
              break;
            case TimestampReason_SCHEDULE:
              ++tick_stats_.rt_delay_schedule_updates_;
              break;
          }
        }
        break;
      }
      case Content_RtRerouteUpdate: {
        ++system_stats_.reroute_updates_;
        ++tick_stats_.rt_reroute_updates_;
        auto const ru = reinterpret_cast<RtRerouteUpdate const*>(u->content());
        update_trip_route(sched, data_, ru, updated_interchange_edges,
                          system_stats_);
        break;
      }
      case Content_RtTrackUpdate: {
        ++tick_stats_.rt_track_updates_;
        break;
      }
      case Content_RtFreeTextUpdate: {
        ++tick_stats_.rt_free_text_updates_;
        break;
      }
      default: break;
    }
  }
  check_broken_interchanges(data_, sched, updated_interchange_edges,
                            system_stats_);
  return {};
}

void paxmon::rt_updates_applied() {
  auto const& sched = get_schedule();
  auto const current_time =
      unix_to_motistime(sched.schedule_begin_, sched.system_time_);
  utl::verify(current_time != INVALID_TIME, "invalid current system time");
  auto const preparation_time = 15 /*min*/;
  auto const search_time = current_time + preparation_time;

  tick_stats_.system_time_ = sched.system_time_;

  auto const affected_passenger_count = std::accumulate(
      begin(data_.groups_affected_by_last_update_),
      end(data_.groups_affected_by_last_update_), 0ULL,
      [](auto const sum, auto const& pg) { return sum + pg->passengers_; });

  tick_stats_.affected_groups_ = data_.groups_affected_by_last_update_.size();
  tick_stats_.affected_passengers_ = affected_passenger_count;

  auto ok_groups = 0ULL;
  auto broken_groups = 0ULL;
  auto broken_passengers = 0ULL;
  {
    scoped_timer timer{"update affected passenger groups"};
    message_creator mc;
    std::vector<flatbuffers::Offset<MonitoringEvent>> fbs_events;

    for (auto const pg : data_.groups_affected_by_last_update_) {
      auto const reachability =
          get_reachability(data_, pg->compact_planned_journey_);
      pg->ok_ = reachability.ok_;

      auto const localization = localize(sched, reachability, search_time);
      update_load(pg, reachability, localization, data_.graph_);

      auto const event_type = reachability.ok_
                                  ? monitoring_event_type::NO_PROBLEM
                                  : monitoring_event_type::TRANSFER_BROKEN;
      fbs_events.emplace_back(
          to_fbs(sched, mc,
                 monitoring_event{event_type, *pg, localization,
                                  reachability.status_}));

      if (reachability.ok_) {
        ++ok_groups;
        ++system_stats_.groups_ok_count_;
        continue;
      }
      ++broken_groups;
      ++system_stats_.groups_broken_count_;
      broken_passengers += pg->passengers_;
    }

    mc.create_and_finish(
        MsgContent_MonitoringUpdate,
        CreateMonitoringUpdate(mc, mc.CreateVector(fbs_events)).Union(),
        "/paxmon/monitoring_update");
    ctx::await_all(motis_publish(make_msg(mc)));
  }

  tick_stats_.ok_groups_ = ok_groups;
  tick_stats_.broken_groups_ = broken_groups;
  tick_stats_.broken_passengers_ = broken_passengers;
  tick_stats_.total_ok_groups_ = system_stats_.groups_ok_count_;
  tick_stats_.total_broken_groups_ = system_stats_.groups_broken_count_;

  LOG(info) << "affected by last rt update: "
            << data_.groups_affected_by_last_update_.size()
            << " passenger groups, "
            << " passengers";

  data_.groups_affected_by_last_update_.clear();
  LOG(info) << "passenger groups: " << ok_groups << " ok, " << broken_groups
            << " broken - passengers affected by broken groups: "
            << broken_passengers;
  LOG(info) << "groups: " << system_stats_.groups_ok_count_ << " ok + "
            << system_stats_.groups_broken_count_ << " broken";

  for (auto const& pg : data_.graph_.passenger_groups_) {
    if (pg->ok_) {
      ++tick_stats_.tracked_ok_groups_;
    } else {
      ++tick_stats_.tracked_broken_groups_;
    }
  }

  stats_writer_->write_tick(tick_stats_);
  stats_writer_->flush();
  tick_stats_ = {};
}

}  // namespace motis::paxmon