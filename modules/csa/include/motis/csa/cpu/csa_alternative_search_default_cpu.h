#pragma once

#include <algorithm>
#include <array>
#include <iostream>
#include <map>

#include "utl/erase_if.h"
#include "utl/verify.h"

#include "motis/core/common/logging.h"

#include "motis/csa/cpu/csa_alternative_base_search.h"
#include "motis/csa/csa_journey.h"
#include "motis/csa/csa_reconstruction.h"
#include "motis/csa/csa_search_shared.h"
#include "motis/csa/csa_statistics.h"
#include "motis/csa/csa_timetable.h"
#include "motis/csa/error.h"

namespace motis::csa::cpu::alternative {

template <search_dir Dir>
struct csa_search : base_search<Dir>{
  static constexpr auto INVALID = Dir == search_dir::FWD
                                      ? std::numeric_limits<time>::max()
                                      : std::numeric_limits<time>::min();

  csa_search(csa_timetable const& tt, time start_time, csa_statistics& stats) : start_time_(start_time), base_search<Dir>(tt,stats){
    std::cout << "CONSTRUCTOR OF CSA SEARCH\n";
  }

  void add_start(csa_station const& station, time initial_duration) {
    auto const station_arrival = Dir == search_dir::FWD
                                     ? start_time_ + initial_duration
                                     : start_time_ - initial_duration;
    this->start_times_[station.id_] = station_arrival;
    this->arrival_time_[station.id_][0] = station_arrival;
    this->stats_.start_count_++;
    expand_footpaths(station, station_arrival, 0);
  }

  void search() {
    auto const& connections =
        Dir == search_dir::FWD ? this->tt_.fwd_connections_ : this->tt_.bwd_connections_;

    csa_connection const start_at{start_time_};
    auto const first_connection = std::lower_bound(
        begin(connections), end(connections), start_at,
        [&](csa_connection const& a, csa_connection const& b) {
          return Dir == search_dir::FWD ? a.departure_ < b.departure_
                                        : a.arrival_ > b.arrival_;
        });
    if (first_connection == end(connections)) {
      return;
    }

    auto const time_limit = Dir == search_dir::FWD
                                ? start_time_ + MAX_TRAVEL_TIME
                                : start_time_ - MAX_TRAVEL_TIME;

    for (auto it = first_connection; it != end(connections); ++it) {
      auto const& con = *it;

      auto& trip_reachable = this->trip_reachable_[con.trip_];
      auto const& from_arrival_time = this->arrival_time_[con.from_station_];
      auto const& to_arrival_time = this->arrival_time_[con.to_station_];

      auto const time_limit_reached = Dir == search_dir::FWD
                                          ? con.departure_ > time_limit
                                          : con.arrival_ < time_limit;
      if (time_limit_reached) {
        break;
      }

      this->stats_.connections_scanned_++;

      for (auto transfers = 0; transfers < LOCAL_MAX_TRANSFERS; ++transfers) {
        auto const via_trip = trip_reachable[transfers];  // NOLINT
        auto const via_station =
            Dir == search_dir::FWD
                ? (from_arrival_time[transfers] <= con.departure_  // NOLINT
                   && con.from_in_allowed_)
                : (to_arrival_time[transfers] >= con.arrival_ &&  // NOLINT
                   con.to_out_allowed_);
        if (via_trip || via_station) {
          trip_reachable[transfers] = true;  // NOLINT
          auto const update =
              Dir == search_dir::FWD
                  ? con.arrival_ < to_arrival_time[transfers + 1] &&  // NOLINT
                        con.to_out_allowed_
                  : (con.departure_ >=
                     from_arrival_time[transfers + 1]) &&  // NOLINT
                        con.from_in_allowed_;
          if (update) {
            this->stats_.footpaths_expanded_++;
            if (Dir == search_dir::FWD) {
              expand_footpaths(this->tt_.stations_[con.to_station_], con.arrival_,
                               transfers + 1);
            } else {
              expand_footpaths(this->tt_.stations_[con.from_station_], con.departure_,
                               transfers + 1);
            }
          }
        }
      }
    }
  }

  void expand_footpaths(csa_station const& station, time station_arrival,
                        int transfers) {
    if (Dir == search_dir::FWD) {
      for (auto const& fp : station.footpaths_) {
        auto const fp_arrival = station_arrival + fp.duration_;
        if (this->arrival_time_[fp.to_station_][transfers] > fp_arrival) {
          this->arrival_time_[fp.to_station_][transfers] = fp_arrival;
        }
      }
    } else {
      for (auto const& fp : station.incoming_footpaths_) {
        auto const fp_arrival = station_arrival - fp.duration_;
        if (this->arrival_time_[fp.from_station_][transfers] < fp_arrival) {
          this->arrival_time_[fp.from_station_][transfers] = fp_arrival;
        }
      }
    }
  }

  std::vector<csa_journey>  get_results(csa_station const& station,
                                       bool include_equivalent) override {
/*    utl::verify_ex(!include_equivalent,
                   std::system_error{error::include_equivalent_not_supported});*/

    std::vector<csa_journey> journeys;
    auto const& station_arrival = this->arrival_time_[station.id_];
    for (auto i = 0; i <= LOCAL_MAX_TRANSFERS; ++i) {
      auto const arrival_time = station_arrival[i];  // NOLINT
      if (arrival_time != INVALID) {
        if(include_equivalent){
          csa_reconstruction<Dir, decltype(this->arrival_time_),
              decltype(this->trip_reachable_)>{
              this->tt_, this->start_times_, this->arrival_time_, this->trip_reachable_}
              .extract_equivalent_journeys(start_time_,
                                                     arrival_time, i, &station, journeys);
        }else {
          csa_reconstruction<Dir, decltype(this->arrival_time_),
              decltype(this->trip_reachable_)>{
              this->tt_, this->start_times_, this->arrival_time_, this->trip_reachable_}
              .extract_journey(journeys.emplace_back(Dir, start_time_,
                                                     arrival_time, i, &station));
        }
      }
    }

    std::cout << "Journey size of " << journeys.size() << " \n";
    return journeys;
  }

  time start_time_;

};

}  // namespace motis::csa::cpu::alternative
