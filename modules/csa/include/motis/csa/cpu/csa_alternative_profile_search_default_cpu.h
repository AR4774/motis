#pragma once

#include <limits>
#include <vector>

#include "utl/verify.h"

#include "motis/core/schedule/interval.h"

#include "motis/csa/cpu/csa_alternative_base_search.h"
#include "motis/csa/csa_journey.h"
#include "motis/csa/csa_search_shared.h"
#include "motis/csa/csa_statistics.h"
#include "motis/csa/csa_timetable.h"
#include "motis/csa/error.h"

namespace motis::csa::cpu::alternative {
template <search_dir Dir>
struct csa_profile_search : base_search<Dir> {
  static constexpr auto INVALID = Dir == search_dir::FWD
                                      ? std::numeric_limits<time>::max()
                                      : std::numeric_limits<time>::min();

  csa_profile_search(csa_timetable const& tt, interval const& search_interval,
                     csa_statistics& stats)
      : search_interval_{search_interval}, base_search<Dir>(tt, stats) {}

  void add_start(csa_station const& station, time initial_duration) {
    // Ready for departure at station at time:
    // start time + initial_duration (Dir == search_dir::FWD)
    // start time - initial_duration (Dir == search_dir::BWD)
    this->stats_.start_count_++;

    (void)station;
    (void)initial_duration;
  }

  void search() {
    auto const& connections = Dir == search_dir::FWD
                                  ? this->tt_.fwd_connections_
                                  : this->tt_.bwd_connections_;

    (void)connections;
  }

  std::vector<csa_journey> get_results(csa_station const& station,
                                       bool include_equivalent) override {
    utl::verify_ex(!include_equivalent,
                   std::system_error{error::include_equivalent_not_supported});

    (void)station;
    return {};
  }

  interval search_interval_;
};

}  // namespace motis::csa::cpu::alternative