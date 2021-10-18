#pragma once

#include "utl/pipes.h"
#include "utl/verify.h"

#include "boost/filesystem.hpp"

#include "motis/core/common/logging.h"
#include "motis/core/access/service_access.h"
#include "motis/csa/csa_search_shared.h"
#include "motis/csa/csa_timetable.h"

#define TFMT "%02d:%02d.%d"
#define FMT_TIME(t)                                             \
  ((t) >= INVALID_TIME ? 0 : (((t) % MINUTES_A_DAY) / 60)),     \
      ((t) >= INVALID_TIME ? 0 : (((t) % MINUTES_A_DAY) % 60)), \
      ((t) >= INVALID_TIME ? 0 : ((t) / MINUTES_A_DAY))

namespace motis::csa {

struct journey_pointer {
  journey_pointer() = default;
  journey_pointer(csa_connection const* enter_con,
                  csa_connection const* exit_con, footpath const* footpath)
      : enter_con_(enter_con), exit_con_(exit_con), footpath_(footpath) {}

  bool valid() const {
    return enter_con_ != nullptr && exit_con_ != nullptr &&
           footpath_ != nullptr;
  }

  csa_connection const* enter_con_{nullptr};
  csa_connection const* exit_con_{nullptr};
  footpath const* footpath_{nullptr};
};

template <search_dir Dir, typename ArrivalTimes, typename TripReachable>
struct csa_reconstruction {
  using arrival_time_t = std::remove_reference_t<
      std::remove_cv_t<decltype(std::declval<ArrivalTimes>()[0][0])>>;

  static constexpr auto INVALID =
      Dir == search_dir::FWD ? std::numeric_limits<arrival_time_t>::max()
                             : std::numeric_limits<arrival_time_t>::min();

  csa_reconstruction(csa_timetable const& tt,
                     std::map<station_id, time> const& start_times,
                     ArrivalTimes const& arrival_time,
                     TripReachable const& trip_reachable)
      : tt_(tt),
        start_times_(start_times),
        arrival_time_(arrival_time),
        trip_reachable_(trip_reachable) {}

  inline bool is_start(station_id station) const {
    return start_times_.find(station) != end(start_times_);
  }

  std::vector<journey_pointer> journey_pointer_correction(
      std::vector<journey_pointer>& journey_pointers,
      const csa_station* destination_station, int transfers) {
    std::vector<journey_pointer> res;
    for (auto& jp : journey_pointers) {
      if (jp.valid()) {
        if (jp.footpath_->from_station_ != jp.footpath_->to_station_) {
          auto valid = 0u;
          auto const con_arr_jps = look_for_conn_arrivals_within_transfer_time(
              destination_station, transfers);
          for (auto const& con_arr_jp : con_arr_jps) {
            if (con_arr_jp.valid()) {
              valid++;
              res.emplace_back(con_arr_jp);
            }
          }
          if (valid == 0) {
            res.emplace_back(jp);
          }
        } else {
          res.emplace_back(jp);
        }
      } else {
        res.emplace_back(jp);
      }
    }
    return res;
  }

  void extract_equivalent_journeys(time start_time, time arrival_time,
                                   unsigned transfers,
                                   csa_station const* destination_station,
                                   std::vector<csa_journey>& journey_list) {
    auto lis = extract_journey_recursion(start_time, arrival_time, transfers,
                                         destination_station, transfers);
    for (auto& j : lis) {
      if (Dir == search_dir::BWD) {
        std::reverse(begin(j.edges_), end(j.edges_));
      }
      journey_list.push_back(j);
    }
  }

  std::vector<csa_journey> extract_journey_recursion(
      time start_time, time arrival_time, unsigned transfers,
      csa_station const* destination_station, unsigned top_transfers) {

    std::vector<csa_journey> result;
    if (transfers == 0) {
      csa_journey j = {Dir, start_time, arrival_time, top_transfers,
                       destination_station};
      j.start_station_ = destination_station;

      if (!is_start(destination_station->id_)) {
        add_final_footpath(j, destination_station, transfers);
      }
      result.emplace_back(j);
      return result;
    }
    auto journey_pointers =
        get_journey_pointers(*destination_station, transfers);
    if (top_transfers == transfers) {
      journey_pointers = journey_pointer_correction(
          journey_pointers, destination_station, transfers);
    }

    for (auto const& jp : journey_pointers) {
      if (jp.valid()) {
        auto cur_list =
            Dir == search_dir::FWD
                ? extract_journey_recursion(
                      start_time, jp.enter_con_->departure_, transfers - 1,
                      &tt_.stations_[jp.enter_con_->from_station_],
                      top_transfers)
                : extract_journey_recursion(
                      start_time, jp.exit_con_->arrival_, transfers - 1,
                      &tt_.stations_[jp.exit_con_->to_station_], top_transfers);
        assert(jp.enter_con_->trip_ == jp.exit_con_->trip_);
        for (auto& j : cur_list) {
          auto const& trip_cons = tt_.trip_to_connections_[jp.exit_con_->trip_];
          auto const add_trip_edge = [&](csa_connection const* con) {
            auto const enter = con == jp.enter_con_;
            auto const exit = con == jp.exit_con_;
            utl::verify(con->light_con_ != nullptr, "invalid light connection");
            j.edges_.emplace_back(con->light_con_,
                                  &tt_.stations_[con->from_station_],
                                  &tt_.stations_[con->to_station_], enter, exit,
                                  con->departure_, con->arrival_);
          };
          if (Dir == search_dir::FWD) {
            auto in_trip = false;
            for (auto const& con : trip_cons) {
              if (con == jp.enter_con_) {
                in_trip = true;
              }
              if (in_trip) {
                add_trip_edge(con);
              }
              if (con == jp.exit_con_) {
                break;
              }
            }
          } else {
            auto in_trip = false;
            for (int i = static_cast<int>(trip_cons.size()) - 1; i >= 0; --i) {
              auto const con = trip_cons[i];
              if (con == jp.exit_con_) {
                in_trip = true;
              }
              if (in_trip) {
                add_trip_edge(con);
              }
              if (con == jp.enter_con_) {
                break;
              }
            }
          }
          if (jp.footpath_->from_station_ != jp.footpath_->to_station_) {
            if (Dir == search_dir::FWD) {
              j.edges_.emplace_back(
                  &tt_.stations_[jp.footpath_->from_station_],
                  &tt_.stations_[jp.footpath_->to_station_],
                  jp.exit_con_->arrival_,
                  jp.exit_con_->arrival_ + jp.footpath_->duration_, -1);
              j.destination_station_ = destination_station;
              j.arrival_time_ = arrival_time;
            } else {
              j.edges_.emplace_back(
                  &tt_.stations_[jp.footpath_->from_station_],
                  &tt_.stations_[jp.footpath_->to_station_],
                  jp.enter_con_->departure_ - jp.footpath_->duration_,
                  jp.enter_con_->departure_, -1);
            }
          }
          if (Dir == search_dir::FWD) {
            j.destination_station_ = destination_station;
            j.arrival_time_ = arrival_time;
          } else {
            j.destination_station_ =
                &tt_.stations_[jp.footpath_->from_station_];
            j.start_time_ = jp.enter_con_->departure_ - jp.footpath_->duration_;
          }
          result.push_back(j);
        }
      } else {
        if (!is_start(destination_station->id_)) {
          if (transfers != 0) {
            LOG(motis::logging::warn)
                << "csa extract journey: adding final footpath "
                   "with transfers="
                << transfers;
          }
          csa_journey j = {Dir, start_time, arrival_time, transfers,
                           destination_station};
          add_final_footpath(j, destination_station, transfers);
          result.push_back(j);
        }
      }
    }

    return result;
  }

  void extract_journey(csa_journey& j) {
    if (j.is_reconstructed()) {
      return;
    }

    auto stop = j.destination_station_;
    auto transfers = j.transfers_;
    for (; transfers > 0; --transfers) {
      auto jp = get_journey_pointer(*stop, transfers);
      if (jp.valid()) {
        if (transfers == j.transfers_ &&
            jp.footpath_->from_station_ != jp.footpath_->to_station_) {
          if (auto const con_arr_jp =
                  look_for_conn_arrival_within_transfer_time(stop, transfers);
              con_arr_jp.valid()) {
            jp = con_arr_jp;
          }
        }

        if (jp.footpath_->from_station_ != jp.footpath_->to_station_) {
          if (Dir == search_dir::FWD) {
            j.edges_.emplace_back(
                &tt_.stations_[jp.footpath_->from_station_],
                &tt_.stations_[jp.footpath_->to_station_],
                jp.exit_con_->arrival_,
                jp.exit_con_->arrival_ + jp.footpath_->duration_, -1);
          } else {
            j.edges_.emplace_back(
                &tt_.stations_[jp.footpath_->from_station_],
                &tt_.stations_[jp.footpath_->to_station_],
                jp.enter_con_->departure_ - jp.footpath_->duration_,
                jp.enter_con_->departure_, -1);
          }
        }
        assert(jp.enter_con_->trip_ == jp.exit_con_->trip_);
        auto const& trip_cons = tt_.trip_to_connections_[jp.exit_con_->trip_];
        auto const add_trip_edge = [&](csa_connection const* con) {
          auto const enter = con == jp.enter_con_;
          auto const exit = con == jp.exit_con_;
          utl::verify(con->light_con_ != nullptr, "invalid light connection");
          j.edges_.emplace_back(con->light_con_,
                                &tt_.stations_[con->from_station_],
                                &tt_.stations_[con->to_station_], enter, exit,
                                con->departure_, con->arrival_);
        };
        if (Dir == search_dir::FWD) {
          auto in_trip = false;
          for (int i = static_cast<int>(trip_cons.size()) - 1; i >= 0; --i) {
            auto const con = trip_cons[i];
            if (con == jp.exit_con_) {
              in_trip = true;
            }
            if (in_trip) {
              add_trip_edge(con);
            }
            if (con == jp.enter_con_) {
              break;
            }
          }
          stop = &tt_.stations_[jp.enter_con_->from_station_];
        } else {
          auto in_trip = false;
          for (auto const& con : trip_cons) {
            if (con == jp.enter_con_) {
              in_trip = true;
            }
            if (in_trip) {
              add_trip_edge(con);
            }
            if (con == jp.exit_con_) {
              break;
            }
          }
          stop = &tt_.stations_[jp.exit_con_->to_station_];
        }
        j.start_station_ = stop;
      } else {
        if (!is_start(stop->id_)) {
          if (transfers != 0) {
            LOG(motis::logging::warn)
                << "csa extract journey: adding final footpath "
                   "with transfers="
                << transfers;
          }
          add_final_footpath(j, stop, transfers);
        }
        break;
      }
    }
    if (transfers == 0 && !is_start(stop->id_)) {
      add_final_footpath(j, stop, transfers);
    }
    if (Dir == search_dir::FWD) {
      std::reverse(begin(j.edges_), end(j.edges_));
    }
  }

  journey_pointer look_for_conn_arrival_within_transfer_time(
      csa_station const* stop, int transfers) {
    for (auto t = 0U; t <= tt_.stations_[stop->id_].transfer_time_; ++t) {
      auto const offset = Dir == search_dir::FWD ? t : -t;
      auto const jp = get_journey_pointer(
          *stop, transfers, arrival_time_[stop->id_][transfers] + offset);
      if (jp.valid()) {
        return jp;
      }
    }
    return {};
  }

  std::vector<journey_pointer> look_for_conn_arrivals_within_transfer_time(
      csa_station const* stop, int transfers) {
    std::vector<journey_pointer> result;
    for (auto t = 0U; t <= tt_.stations_[stop->id_].transfer_time_; ++t) {
      auto const offset = Dir == search_dir::FWD ? t : -t;
      auto const jp = get_journey_pointers(
          *stop, transfers, arrival_time_[stop->id_][transfers] + offset);
      for (auto j : jp) {
        if (j.valid()) {
          result.emplace_back(j);
        }
      }
    }
    return result;
  }

  void add_final_footpath(csa_journey& j, csa_station const* stop,
                          int transfers) {
    assert(transfers == 0);
    auto const fp_arrival = arrival_time_[stop->id_][transfers];
    if (Dir == search_dir::FWD) {
      for (auto const& fp : stop->incoming_footpaths_) {
        if (fp.from_station_ == fp.to_station_) {
          continue;
        }
        auto const fp_departure = fp_arrival - fp.duration_;
        auto const valid_station = is_start(fp.from_station_);
        if (valid_station &&
            fp_departure >= start_times_.at(fp.from_station_)) {
          j.edges_.emplace_back(&tt_.stations_[fp.from_station_],
                                &tt_.stations_[fp.to_station_], fp_departure,
                                fp_arrival, -1);
          j.start_station_ = &tt_.stations_[fp.to_station_];
          break;
        }
      }
    } else {
      for (auto const& fp : stop->footpaths_) {
        if (fp.from_station_ == fp.to_station_) {
          continue;
        }
        auto const fp_departure = fp_arrival + fp.duration_;
        auto const valid_station = is_start(fp.to_station_);
        if (valid_station && fp_departure <= start_times_.at(fp.to_station_)) {
          j.edges_.emplace_back(&tt_.stations_[fp.from_station_],
                                &tt_.stations_[fp.to_station_], fp_arrival,
                                fp_departure, -1);
          j.start_station_ = &tt_.stations_[fp.from_station_];
          break;
        }
      }
    }
  }

  std::vector<journey_pointer> get_journey_pointers(
      csa_station const& station, int transfers,
      time const station_arrival_override = INVALID) {

    auto const is_override_active = station_arrival_override != INVALID;
    auto const& station_arrival = is_override_active
                                      ? station_arrival_override
                                      : arrival_time_[station.id_][transfers];
    std::vector<journey_pointer> result;

    if (Dir == search_dir::FWD) {
      for (auto const& fp : station.incoming_footpaths_) {
        if (is_override_active && fp.from_station_ != fp.to_station_) {
          continue;  // Override => we are looking for connection arrivals.
        }

        auto const& fp_dep_stop = tt_.stations_[fp.from_station_];
        for (auto const& exit_con : get_exit_candidates(
                 fp_dep_stop, station_arrival - fp.duration_, transfers)) {
          for (auto const& enter_con :
               tt_.trip_to_connections_[exit_con->trip_]) {
            if (arrival_time_[enter_con->from_station_][transfers - 1] <=
                    enter_con->departure_ &&
                enter_con->from_in_allowed_) {
              result.emplace_back(enter_con, exit_con, &fp);
              break;
            }
            if (enter_con == exit_con) {
              break;
            }
          }
        }
      }
    } else {
      for (auto const& fp : station.footpaths_) {
        auto const& fp_arr_stop = tt_.stations_[fp.to_station_];
        for (auto const& enter_con : get_enter_candidates(
                 fp_arr_stop, station_arrival + fp.duration_, transfers)) {
          auto const& trip_cons = tt_.trip_to_connections_[enter_con->trip_];
          for (auto i = static_cast<int>(trip_cons.size()) - 1; i >= 0; --i) {
            auto const& exit_con = trip_cons[i];
            auto const exit_arrival =
                arrival_time_[exit_con->to_station_][transfers - 1];
            if (exit_arrival != INVALID && exit_arrival >= exit_con->arrival_ &&
                exit_con->to_out_allowed_) {
              result.emplace_back(enter_con, exit_con, &fp);
              break;
            }
            if (exit_con == enter_con) {
              break;
            }
          }
        }
      }
    }

    return result;
  }

  journey_pointer get_journey_pointer(
      csa_station const& station, int transfers,
      time const station_arrival_override = INVALID) {
    auto const is_override_active = station_arrival_override != INVALID;
    auto const& station_arrival = is_override_active
                                      ? station_arrival_override
                                      : arrival_time_[station.id_][transfers];
    if (Dir == search_dir::FWD) {
      for (auto const& fp : station.incoming_footpaths_) {
        if (is_override_active && fp.from_station_ != fp.to_station_) {
          continue;  // Override => we are looking for connection arrivals.
        }

        auto const& fp_dep_stop = tt_.stations_[fp.from_station_];
        for (auto const& exit_con : get_exit_candidates(
                 fp_dep_stop, station_arrival - fp.duration_, transfers)) {
          for (auto const& enter_con :
               tt_.trip_to_connections_[exit_con->trip_]) {
            if (arrival_time_[enter_con->from_station_][transfers - 1] <=
                    enter_con->departure_ &&
                enter_con->from_in_allowed_) {
              return {enter_con, exit_con, &fp};
            }
            if (enter_con == exit_con) {
              break;
            }
          }
        }
      }
    } else {
      for (auto const& fp : station.footpaths_) {
        auto const& fp_arr_stop = tt_.stations_[fp.to_station_];
        for (auto const& enter_con : get_enter_candidates(
                 fp_arr_stop, station_arrival + fp.duration_, transfers)) {
          auto const& trip_cons = tt_.trip_to_connections_[enter_con->trip_];

          for (auto i = static_cast<int>(trip_cons.size()) - 1; i >= 0; --i) {
            auto const& exit_con = trip_cons[i];
            auto const exit_arrival =
                arrival_time_[exit_con->to_station_][transfers - 1];

            if (exit_arrival != INVALID && exit_arrival >= exit_con->arrival_ &&
                exit_con->to_out_allowed_) {
              return {enter_con, exit_con, &fp};
            }
            if (exit_con == enter_con) {
              break;
            }
          }
        }
      }
    }

    return {};
  }

  auto get_exit_candidates(csa_station const& arrival_station,
                           time arrival_time, int transfers) const {
    return utl::all(arrival_station.incoming_connections_)  //
           | utl::remove_if(
                 [this, arrival_time, transfers](csa_connection const* con) {
                   return con->arrival_ != arrival_time ||
                          !trip_reachable_[con->trip_][transfers - 1] ||
                          !con->to_out_allowed_;
                 })  //
           | utl::iterable();
  }

  auto get_enter_candidates(csa_station const& departure_station,
                            time departure_time, int transfers) const {
    return utl::all(departure_station.outgoing_connections_)  //
           | utl::remove_if(
                 [this, departure_time, transfers](csa_connection const* con) {
                   return con->departure_ != departure_time ||
                          !trip_reachable_[con->trip_][transfers - 1] ||
                          !con->from_in_allowed_;
                 })  //
           | utl::iterable();
  }

  csa_timetable const& tt_;
  std::map<station_id, time> const& start_times_;
  ArrivalTimes const& arrival_time_;
  TripReachable const& trip_reachable_;
};

}  // namespace motis::csa
