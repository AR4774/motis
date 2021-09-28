#pragma once

#include <algorithm>
#include "motis/csa/cpu/csa_alternative_base_search.h"
#include "motis/module/module.h"

#include "motis/csa/csa_implementation_type.h"
#include "../../../../tripbased/include/motis/tripbased/tb_profile_search.h"
#include "csa_journey.h"
#include "run_csa_search.h"

#include "utl/pipes.h"


namespace motis::csa {
  struct csa_alternative_search {


    response alternative_search(schedule const& sched, csa_timetable const& tt,
                                csa_query const& q, SearchType search_type,
                                implementation_type impl_type,
                                bool use_profile_search) const {

      auto fwd_q = q;
      auto bwd_q = q;
      // switch such that the bwd search takes t as starting point (as both cases take the departure station as starting point)
      bwd_q.meta_starts_ = fwd_q.meta_dests_;
      bwd_q.meta_dests_ = fwd_q.meta_starts_;

      if (q.dir_ == search_dir::FWD) {
        bwd_q.dir_ = search_dir::BWD;
      } else {
        fwd_q.dir_ = search_dir::FWD;
      }

      // ToDo: calculate the offset by extracting an optimal journey and adding some percentage/constant
      bwd_q.search_interval_.begin_ += 14 * 3600;
      bwd_q.search_interval_.end_ += 14 * 3600;
      fwd_q.search_interval_.begin_ -= 1 * 3600;
      fwd_q.search_interval_.end_ -= 1 * 3600;
      csa_statistics stats;

      std::cout << "Begin " << bwd_q.search_interval_.begin_ << "\n";
      std::cout << "End " << bwd_q.search_interval_.end_ << "\n";

/*      auto fwd_r =
          !use_profile_search
          ? pretrip_alternative<pretrip_iterated_ontrip_alternative_search<
              cpu::alternative::csa_search<search_dir::FWD>>,
              search_dir::FWD>(sched, tt, fwd_q, stats)
                  .search()
          : pretrip_alternative<
            pretrip_profile_alternative_search<
                cpu::alternative::csa_profile_search<search_dir::FWD>,
                cpu::alternative::csa_search<search_dir::FWD>>,
          search_dir::FWD>(sched, tt, fwd_q, stats)
              .search();*/

      auto fwd_r =
          !use_profile_search
          ? pretrip_alternative<pretrip_iterated_ontrip_alternative_search<
              cpu::alternative::csa_search<search_dir::FWD>>,
              search_dir::FWD>{sched, tt, fwd_q, stats}
              .search()
          : pretrip_alternative<
              pretrip_profile_alternative_search<
                  cpu::alternative::csa_profile_search<search_dir::FWD>,
                  cpu::alternative::csa_search<search_dir::FWD>>,
              search_dir::FWD>{sched, tt, fwd_q, stats}
              .search();

      std::cout << "BACKWARD SEARCH STARTS \n";


      /*auto bwd_r =
          !use_profile_search
          ? pretrip_alternative<pretrip_iterated_ontrip_alternative_search<cpu::alternative::csa_search<search_dir::BWD>>,
              search_dir::BWD>(sched, tt, bwd_q, stats)
                  .search()
          : pretrip_alternative<
            pretrip_profile_alternative_search<
                cpu::alternative::csa_profile_search<search_dir::BWD>,
                cpu::alternative::csa_search<search_dir::BWD>>,
          search_dir::BWD>(sched, tt, bwd_q, stats)
              .search();*/

      auto bwd_r =
          !use_profile_search
          ? pretrip_alternative<pretrip_iterated_ontrip_alternative_search<cpu::alternative::csa_search<search_dir::BWD>>,
              search_dir::BWD>{sched, tt, bwd_q, stats}.search()
          : pretrip_alternative<
              pretrip_profile_alternative_search<
                  cpu::alternative::csa_profile_search<search_dir::BWD>,
                  cpu::alternative::csa_search<search_dir::BWD>>,
              search_dir::BWD>{sched, tt, bwd_q, stats}.search();

      std::cout << " size of backward " << bwd_r.size() << "\n" ;
      std::cout << " equal null " << (bwd_r[0].get() == nullptr) << "\n" ;

      std::vector<csa_journey> result;

      auto testLis =
          run_csa_search(sched, tt, bwd_q, search_type, impl_type, false).journeys_;
      std::cout << "Example journey starts at " << testLis.front().journey_begin()
                << " and "
                << " ends at " << testLis.front().journey_end() << "\n";
      std::vector<csa_station> stationTestLis = std::vector<csa_station>();
      for (auto& e : testLis) {
        for (auto s : e.edges_) {
          stationTestLis.push_back(*s.to_);
        }
      }


      // TODO: Should the direct journeys be added to the result?
      for (auto const& station : stationTestLis) {


        std::vector<std::vector<int>> valid_search_pairs;
        std::vector<bool> valid_bwd_searches(bwd_r.size());
        std::fill(begin(valid_bwd_searches), end(valid_bwd_searches), false);

        auto station_valid = false;

        for (auto k = 0; k < fwd_r.size(); ++k) {
          valid_search_pairs.emplace_back();
          auto const& search = *fwd_r[k];
          auto const& station_arrival = search.arrival_time_[station.id_];
          for (auto l = 0; l < bwd_r.size(); ++l) {
            auto const& search_b = *bwd_r[l];
            auto const& station_arrival_b = search_b.arrival_time_[station.id_];
            for (auto i = 0; i <= cpu::alternative::LOCAL_MAX_TRANSFERS; ++i) {
              auto arrival_time = station_arrival[i];
              if (arrival_time == cpu::alternative::base_search<search_dir::FWD>::INVALID) {
                continue;
              }
              if (std::any_of(begin(station_arrival_b), end(station_arrival_b),
                              [&](auto const& ar) {
                                std::cout << " ar b " << ar << " and ar a " << arrival_time << "\n";
                                return ar != cpu::alternative::base_search<search_dir::BWD>::INVALID && ar >= arrival_time;
                              })) {
                std::cout << "Any of returned true for " << k << " and " << l << " \n" ;
                valid_bwd_searches[l] = true;
                valid_search_pairs[k].push_back(l);
                station_valid = true;
                break;
              }
            }
          }
        }

        if (!station_valid) {
          continue;
        }

        std::cout << "STATION IS VALID FOR STATION" << station.station_ptr_->name_ << "\n";

        auto journeys_fwd = std::vector<std::vector<csa_journey>>();
        auto journeys_bwd = std::vector<std::vector<csa_journey>>();


        std::vector<bool> valid_fwd_searches;
        std::transform(begin(valid_search_pairs), end(valid_search_pairs), std::back_inserter(valid_fwd_searches) , [&](auto const& p){return !p.empty();});

        std::cout << "START CONSTRUCTING VALID JOURNEYS \n";
        construct_valid_journeys(fwd_r, valid_fwd_searches, q, station, journeys_fwd);
        construct_valid_journeys(bwd_r, valid_bwd_searches, q, station, journeys_bwd);

        for (auto k = 0; k < valid_search_pairs.size(); ++k) {
          auto const lis = valid_search_pairs[k];
          std::for_each(begin(lis), end(lis), [&](auto const& l) {
            std::cout << "COMBINING\n";
            auto combined = combine_journeys(journeys_fwd[k], journeys_bwd[l]);
            std::for_each(begin(combined), end(combined),
                          [&](auto const& j) { result.push_back(j); });
          });
        }
      }



      sort(begin(result), end(result));
      auto last = std::unique(begin(result), end(result), [](csa_journey const& a, csa_journey const& b){
        auto ab = a.journey_begin(); auto ae = a.journey_end();
        auto bb = b.journey_begin(); auto be = b.journey_end();
        return std::tie(ab,ae,a.start_station_, a.destination_station_,a.edges_) == std::tie(bb,be,b.start_station_,b.destination_station_,b.edges_);
      });
      result.erase(last, end(result));

      // ToDo: check whether stats need to be combined from fwd and bwd
      response resp = {stats, result, bwd_q.search_interval_};
      return resp;
    }


    template<search_dir Dir>
    void construct_valid_journeys(std::vector<std::unique_ptr<motis::csa::cpu::alternative::base_search<Dir>>>& csa_searches,
                                  std::vector<bool>& valid_searches,csa_query const& q, csa_station const& station, std::vector<std::vector<csa_journey>>& journeys) const {
      for (auto k = 0; k < csa_searches.size(); ++k) {
        if (!valid_searches[k]) {
          std::cout << "NOT VALID " << k  << "\n";
          journeys.emplace_back();
          continue;
        }
        std::cout << "VALID CONSTRUCTION\n";

        auto cur_journeys =
            csa_searches[k]->get_results(station, q.include_equivalent_);
        std::cout << cur_journeys.size() << " RESULT SIZE\n";

        auto is_dir_fwd = Dir == search_dir::FWD;
        auto targets = is_dir_fwd ? q.meta_starts_ : q.meta_dests_;

        utl::erase_if(cur_journeys, [&](csa_journey const& j) {
          std::cout  <<  (std::find(begin(targets), end(targets),
                                    (is_dir_fwd ? j.departure_station()->id_ : j.arrival_station()->id_)) == end(q.meta_starts_) ? " ERASING\n" : "");

          return std::find(begin(targets), end(targets),
                           (is_dir_fwd ? j.departure_station()->id_ : j.arrival_station()->id_))
                 == end(q.meta_starts_);
        });

        std::cout << " unique size " << cur_journeys.size() << "\n";
        journeys.push_back(cur_journeys);
      }
    }

    static std::vector<csa_journey> combine_journeys(std::vector<csa_journey>& fwd,
                                              std::vector<csa_journey>& bwd) {
      auto result = std::vector<csa_journey>();

      for(auto const& cur : fwd){
        for(auto const& cur_b : bwd){
          if (cur_b.edges_.back().departure_ < cur.edges_.front().arrival_) {
            std::cout << "TIMING INVALID \n";
            continue;
          }
          std::cout << "TIMING VALID \n";

          // combine both journeys into a new journey
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

  };

}