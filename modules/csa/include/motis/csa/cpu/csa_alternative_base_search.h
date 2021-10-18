#pragma once

#include <limits>
#include <vector>

#include "utl/verify.h"

#include "motis/core/schedule/interval.h"

#include "motis/core/schedule/edges.h"
#include "motis/csa/csa_journey.h"
#include "motis/csa/csa_search_shared.h"
#include "motis/csa/csa_statistics.h"
#include "motis/csa/csa_timetable.h"
#include "motis/csa/error.h"

namespace motis::csa::cpu::alternative {
const duration LOCAL_MAX_TRANSFERS = 3;

template <search_dir Dir>
struct base_search {
  static constexpr auto INVALID = Dir == search_dir::FWD
                                      ? std::numeric_limits<time>::max()
                                      : std::numeric_limits<time>::min();

  base_search(csa_timetable const& tt, csa_statistics& stats)
      : tt_(tt),
        arrival_time_(
            tt.stations_.size(),
            array_maker<time, LOCAL_MAX_TRANSFERS + 1>::make_array(INVALID)),
        trip_reachable_(tt.trip_count_),
        stats_(stats) {}

  virtual std::vector<csa_journey> get_results(csa_station const& station,
                                               bool include_equivalent) {
    return std::vector<csa_journey>();
  };

  csa_timetable const& tt_;
  std::map<station_id, time> start_times_;
  std::vector<std::array<time, LOCAL_MAX_TRANSFERS + 1>> arrival_time_;
  std::vector<std::array<bool, LOCAL_MAX_TRANSFERS + 1>> trip_reachable_;
  csa_statistics& stats_;
};

}  // namespace motis::csa::cpu::alternative