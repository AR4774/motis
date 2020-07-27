#include "motis/paxmon/capacity.h"

#include <charconv>
#include <cstdint>
#include <ctime>
#include <fstream>
#include <functional>
#include <iterator>
#include <optional>
#include <set>
#include <string>
#include <utility>

#include "utl/parser/buf_reader.h"
#include "utl/parser/csv_range.h"
#include "utl/parser/file.h"
#include "utl/parser/line_range.h"
#include "utl/pipes/for_each.h"

#include "motis/core/common/logging.h"
#include "motis/core/access/station_access.h"

#include "motis/paxmon/util/get_station_idx.h"

using namespace motis::logging;
using namespace motis::paxmon::util;

namespace motis::paxmon {

namespace {

struct row {
  utl::csv_col<std::uint32_t, UTL_NAME("train_nr")> train_nr_;
  utl::csv_col<utl::cstr, UTL_NAME("category")> category_;
  utl::csv_col<utl::cstr, UTL_NAME("from")> from_;
  utl::csv_col<utl::cstr, UTL_NAME("from_name")> from_name_;
  utl::csv_col<utl::cstr, UTL_NAME("to")> to_;
  utl::csv_col<utl::cstr, UTL_NAME("to_name")> to_name_;
  utl::csv_col<std::time_t, UTL_NAME("departure")> departure_;
  utl::csv_col<std::time_t, UTL_NAME("arrival")> arrival_;
  utl::csv_col<std::uint16_t, UTL_NAME("seats")> seats_;
};

};  // namespace

std::size_t load_capacities(schedule const& sched,
                            std::string const& capacity_file,
                            trip_capacity_map_t& trip_map,
                            category_capacity_map_t& category_map,
                            std::string const& match_log_file) {
  auto buf = utl::file(capacity_file.data(), "r").content();
  auto const file_content = utl::cstr{buf.data(), buf.size()};
  auto entry_count = 0ULL;

  std::set<std::pair<std::string, std::string>> stations_not_found;

  utl::line_range<utl::buf_reader>{file_content}  //
      | utl::csv<row>()  //
      | utl::for_each([&](auto&& row) {
          if (row.train_nr_ != 0) {
            auto const from_station_idx =
                get_station_idx(sched, row.from_.val().view()).value_or(0);
            auto const to_station_idx =
                get_station_idx(sched, row.to_.val().view()).value_or(0);
            time departure = row.departure_.val() != 0
                                 ? unix_to_motistime(sched.schedule_begin_,
                                                     row.departure_.val())
                                 : 0;
            time arrival = row.arrival_.val() != 0
                               ? unix_to_motistime(sched.schedule_begin_,
                                                   row.arrival_.val())
                               : 0;

            if (row.from_.val() && from_station_idx == 0) {
              stations_not_found.insert(std::make_pair(
                  row.from_.val().to_str(), row.from_name_.val().to_str()));
            }
            if (row.to_.val() && to_station_idx == 0) {
              stations_not_found.insert(std::make_pair(
                  row.to_.val().to_str(), row.to_name_.val().to_str()));
            }
            if (departure == INVALID_TIME || arrival == INVALID_TIME) {
              return;
            }

            auto const tid = cap_trip_id{row.train_nr_.val(), from_station_idx,
                                         to_station_idx, departure, arrival};
            trip_map[tid] = row.seats_.val();
            ++entry_count;
          } else if (row.category_.val()) {
            category_map[row.category_.val().view()] = row.seats_.val();
            ++entry_count;
          }
        });

  if (!stations_not_found.empty()) {
    LOG(warn) << stations_not_found.size() << " stations not found";
    if (!match_log_file.empty()) {
      std::ofstream ml{match_log_file};
      ml << "stations not found:\n";
      for (auto const& [id, name] : stations_not_found) {
        ml << id << ": " << name << "\n";
      }
      LOG(warn) << "capacity match log report written to: " << match_log_file;
    }
  }

  return entry_count;
}

std::pair<std::uint16_t, capacity_source> get_capacity(
    schedule const& sched, light_connection const& lc,
    trip_capacity_map_t const& trip_map,
    category_capacity_map_t const& category_map,
    std::uint16_t default_capacity) {
  std::pair<std::uint16_t, capacity_source> result = {
      0, capacity_source::TRAIN_NR};

  auto const find_by_trip = [&](std::function<std::optional<std::uint32_t>(
                                    trip const*)> const& get_train_nr) {
    for (auto const& trp : *sched.merged_trips_.at(lc.trips_)) {
      auto const maybe_trainr_nr = get_train_nr(trp);
      if (!maybe_trainr_nr) {
        continue;
      }
      auto const train_nr = maybe_trainr_nr.value();
      auto const tid = cap_trip_id{train_nr, trp->id_.primary_.get_station_id(),
                                   trp->id_.secondary_.target_station_id_,
                                   trp->id_.primary_.get_time(),
                                   trp->id_.secondary_.target_time_};
      if (auto const lb = trip_map.lower_bound(tid); lb != end(trip_map)) {
        if (lb->first == tid) {
          result = {lb->second, capacity_source::TRIP_EXACT};
          return;
        } else if (lb->first.train_nr_ == train_nr) {
          result.first = lb->second;
        } else if (auto const prev = std::prev(lb);
                   prev != end(trip_map) && prev->first.train_nr_ == train_nr) {
          result.first = prev->second;
        }
      }
    }
  };

  find_by_trip(
      [](trip const* trp) { return trp->id_.primary_.get_train_nr(); });
  if (result.first != 0) {
    return result;
  }

  find_by_trip([](trip const* trp) -> std::optional<std::uint32_t> {
    auto const& line_id = trp->id_.secondary_.line_id_;
    std::uint32_t line_nr = 0;
    auto const result = std::from_chars(
        line_id.data(), line_id.data() + line_id.size(), line_nr);
    if (result.ec == std::errc{} &&
        result.ptr == line_id.data() + line_id.size()) {
      return {line_nr};
    } else {
      return {};
    }
  });
  if (result.first != 0) {
    return result;
  }

  auto const& category =
      sched.categories_[lc.full_con_->con_info_->family_]->name_;
  if (auto const it = category_map.find(category); it != end(category_map)) {
    return {it->second, capacity_source::CATEGORY};
  } else if (auto const it = category_map.find(std::to_string(
                 static_cast<service_class_t>(lc.full_con_->clasz_)));
             it != end(category_map)) {
    return {it->second, capacity_source::CLASZ};
  }
  return {default_capacity, capacity_source::DEFAULT};
}

}  // namespace motis::paxmon