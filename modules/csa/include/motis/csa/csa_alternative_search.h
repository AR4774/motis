#pragma once

#include "motis/module/module.h"
#include "motis/csa/cpu/csa_alternative_base_search.h"
#include <algorithm>

#include "motis/csa/csa_implementation_type.h"
#include "../../../../tripbased/include/motis/tripbased/tb_profile_search.h"
#include "csa_journey.h"
#include "run_csa_search.h"

#include "utl/pipes.h"

namespace motis::csa {
struct csa_alternative_search {
  static void cap_journey_list(std::vector<csa_journey>& result, int cap_size) {
    sort(begin(result), end(result));
    auto last =
        std::unique(begin(result), end(result),
                    [](csa_journey const& a, csa_journey const& b) {
                      auto ab = a.journey_begin();
                      auto ae = a.journey_end();
                      auto bb = b.journey_begin();
                      auto be = b.journey_end();
                      return std::tie(ab, ae, a.start_station_,
                                      a.destination_station_, a.edges_) ==
                             std::tie(bb, be, b.start_station_,
                                      b.destination_station_, b.edges_);
                    });
    result.erase(last, end(result));
    if (result.size() > cap_size) {
      result.resize(cap_size);
    }
  }

  response alternative_search(schedule const& sched, csa_timetable const& tt,
                              csa_query const& q, SearchType search_type,
                              implementation_type impl_type,
                              bool use_profile_search) const {
    MOTIS_START_TIMING(total_timing);

    auto fwd_q = q;
    auto bwd_q = q;
    bwd_q.meta_starts_ = fwd_q.meta_dests_;
    bwd_q.meta_dests_ = fwd_q.meta_starts_;

    if (q.dir_ == search_dir::FWD) {
      bwd_q.dir_ = search_dir::BWD;
    } else {
      fwd_q.dir_ = search_dir::FWD;
    }

    auto journeys_optimal =
        run_csa_search(sched, tt, fwd_q, search_type, impl_type, false)
            .journeys_;
    if (journeys_optimal.empty()) {
      return {};
    }

    std::vector<int> durations;
    transform(begin(journeys_optimal), end(journeys_optimal),
              back_inserter(durations),
              [&](auto const& j) -> unsigned int { return j.duration(); });
    bwd_q.search_interval_.begin_ +=
        (*min_element(begin(durations), end(durations)));
    bwd_q.search_interval_.end_ +=
        (*max_element(begin(durations), end(durations)) + 60);
    csa_statistics stats;

    auto fwd_r =
        !use_profile_search
            ? pretrip_alternative<
                  pretrip_iterated_ontrip_alternative_search<
                      cpu::alternative::csa_search<search_dir::FWD>>,
                  search_dir::FWD>{sched, tt, fwd_q, stats}
                  .search()
            : pretrip_alternative<
                  pretrip_profile_alternative_search<
                      cpu::alternative::csa_profile_search<search_dir::FWD>,
                      cpu::alternative::csa_search<search_dir::FWD>>,
                  search_dir::FWD>{sched, tt, fwd_q, stats}
                  .search();
    auto bwd_r =
        !use_profile_search
            ? pretrip_alternative<
                  pretrip_iterated_ontrip_alternative_search<
                      cpu::alternative::csa_search<search_dir::BWD>>,
                  search_dir::BWD>{sched, tt, bwd_q, stats}
                  .search()
            : pretrip_alternative<
                  pretrip_profile_alternative_search<
                      cpu::alternative::csa_profile_search<search_dir::BWD>,
                      cpu::alternative::csa_search<search_dir::BWD>>,
                  search_dir::BWD>{sched, tt, bwd_q, stats}
                  .search();

    std::vector<csa_journey> result;
    for (auto const& station : tt.stations_) {
      if (find(begin(q.meta_starts_), end(q.meta_starts_), station.id_) !=
              end(q.meta_starts_) ||
          find(begin(q.meta_dests_), end(q.meta_dests_), station.id_) !=
              end(q.meta_dests_)) {
        continue;
      }
      std::vector<std::vector<int>> valid_search_pairs;
      std::vector<bool> valid_bwd_searches(bwd_r.size());
      std::fill(begin(valid_bwd_searches), end(valid_bwd_searches), false);

      auto station_valid = false;

      for (auto const& cur_fs : fwd_r) {
        auto& cur_search_pair = valid_search_pairs.emplace_back();
        auto const& station_arrival = cur_fs->arrival_time_[station.id_];
        auto l = 0u;
        for (auto const& cur_bs : bwd_r) {
          auto const& station_arrival_b = cur_bs->arrival_time_[station.id_];
          for (auto i = 0; i <= cpu::alternative::LOCAL_MAX_TRANSFERS; ++i) {
            auto arrival_time = station_arrival[i];
            if (arrival_time ==
                cpu::alternative::base_search<search_dir::FWD>::INVALID) {
              continue;
            }
            if (std::any_of(begin(station_arrival_b), end(station_arrival_b),
                            [&](auto const& ar) {
                              return ar != cpu::alternative::base_search<
                                               search_dir::BWD>::INVALID &&
                                     ar >= arrival_time;
                            })) {
              valid_bwd_searches[l] = true;
              cur_search_pair.push_back(l);
              station_valid = true;
              break;
            }
          }
          l++;
        }
      }

      if (!station_valid) {
        continue;
      }
      auto journeys_fwd = std::vector<std::vector<csa_journey>>();
      auto journeys_bwd = std::vector<std::vector<csa_journey>>();

      std::vector<bool> valid_fwd_searches;
      std::transform(begin(valid_search_pairs), end(valid_search_pairs),
                     std::back_inserter(valid_fwd_searches),
                     [&](auto const& p) { return !p.empty(); });

      construct_valid_journeys(fwd_r, valid_fwd_searches, fwd_q, station,
                               journeys_fwd);
      construct_valid_journeys(bwd_r, valid_bwd_searches, bwd_q, station,
                               journeys_bwd);

      std::vector<csa_journey> cur_j;
      for (auto k = 0; k < valid_search_pairs.size(); ++k) {
        auto const lis = valid_search_pairs[k];
        std::for_each(begin(lis), end(lis), [&](auto const& l) {
          auto combined = combine_journeys(journeys_fwd[k], journeys_bwd[l]);
          std::for_each(begin(combined), end(combined),
                        [&](auto const& j) { cur_j.push_back(j); });
        });
      }
      cap_journey_list(cur_j, 50);
      result.insert(end(result), begin(cur_j), end(cur_j));
    }

    cap_journey_list(result, 10);
    MOTIS_STOP_TIMING(total_timing);
    stats.total_duration_ = MOTIS_TIMING_MS(total_timing);
    response resp = {stats, result, fwd_q.search_interval_};
    return resp;
  }

  template <search_dir Dir>
  void construct_valid_journeys(
      std::vector<std::unique_ptr<
          motis::csa::cpu::alternative::base_search<Dir>>> const& csa_searches,
      std::vector<bool> const& valid_searches, csa_query const& q,
      csa_station const& station,
      std::vector<std::vector<csa_journey>>& journeys) const {
    for (auto k = 0; k < csa_searches.size(); ++k) {
      if (!valid_searches[k]) {
        journeys.emplace_back();
        continue;
      }

      auto cur_journeys = csa_searches[k]->get_results(station, true);
      auto is_dir_fwd = Dir == search_dir::FWD;
      auto targets = q.meta_starts_;

      utl::erase_if(cur_journeys, [&](auto const& j) {
        return std::find(begin(targets), end(targets),
                         (is_dir_fwd ? j.departure_station()->id_
                                     : j.arrival_station()->id_)) ==
               end(targets);
      });

      utl::erase_if(cur_journeys, [&](auto const& j) {
        return !in_interval(j, q.search_interval_);
      });

      journeys.push_back(cur_journeys);
    }
  }

  static std::vector<csa_journey> combine_journeys(
      std::vector<csa_journey> const& fwd,
      std::vector<csa_journey> const& bwd) {
    auto result = std::vector<csa_journey>();

    for (auto const& cur : fwd) {
      if (!cur.is_reconstructed()) {
        continue;
      }
      for (auto const& cur_b : bwd) {
        if (!cur_b.is_reconstructed() ||
            cur_b.edges_.front().departure_ < cur.edges_.back().arrival_) {
          continue;
        }
        csa_journey combined = {cur.dir_, cur.start_time_, cur_b.arrival_time_,
                                cur.transfers_ + cur_b.transfers_,
                                cur_b.destination_station_};
        combined.start_station_ = cur.start_station_;
        combined.edges_ = std::vector<csa_journey::csa_edge>();
        combined.edges_.insert(end(combined.edges_), begin(cur.edges_),
                               end(cur.edges_));
        combined.edges_.insert(end(combined.edges_), begin(cur_b.edges_),
                               end(cur_b.edges_));
        result.push_back(combined);
      }
    }
    return result;
  }

private:
  bool in_interval(csa_journey const& j, interval search_interval) const {
    auto const begin =
        j.dir_ == search_dir::FWD ? j.journey_begin() : j.journey_end();
    return begin >= search_interval.begin_ && begin <= search_interval.end_;
  }
};

}  // namespace motis::csa