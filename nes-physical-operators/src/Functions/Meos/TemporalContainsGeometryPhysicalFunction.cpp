/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <Functions/Meos/TemporalContainsGeometryPhysicalFunction.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <MEOSWrapper.hpp>
#include <fmt/format.h>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <ExecutionContext.hpp>
#include <ErrorHandling.hpp>
#include <iostream>

namespace NES {

/* ─────────── constructors ─────────── */

TemporalContainsGeometryPhysicalFunction::
TemporalContainsGeometryPhysicalFunction(PhysicalFunction lonFunction,
                                         PhysicalFunction latFunction,
                                         PhysicalFunction timestampFunction,
                                         PhysicalFunction staticGeometryFunction)
    : isTemporal6Param(false)
{
    paramFns.reserve(4);
    paramFns.push_back(std::move(lonFunction));
    paramFns.push_back(std::move(latFunction));
    paramFns.push_back(std::move(timestampFunction));
    paramFns.push_back(std::move(staticGeometryFunction));
}

TemporalContainsGeometryPhysicalFunction::
TemporalContainsGeometryPhysicalFunction(PhysicalFunction lon1Function,
                                         PhysicalFunction lat1Function,
                                         PhysicalFunction timestamp1Function,
                                         PhysicalFunction lon2Function,
                                         PhysicalFunction lat2Function,
                                         PhysicalFunction timestamp2Function)
    : isTemporal6Param(true)
{
    paramFns.reserve(6);
    paramFns.push_back(std::move(lon1Function));
    paramFns.push_back(std::move(lat1Function));
    paramFns.push_back(std::move(timestamp1Function));
    paramFns.push_back(std::move(lon2Function));
    paramFns.push_back(std::move(lat2Function));
    paramFns.push_back(std::move(timestamp2Function));
}

/* ─────────── dispatch ─────────── */

VarVal TemporalContainsGeometryPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    std::cout << "TemporalContainsGeometryPhysicalFunction::execute called with "
              << paramFns.size() << " arguments" << std::endl;

    std::vector<VarVal> vals;
    vals.reserve(paramFns.size());
    for (const auto& f : paramFns) {
        vals.push_back(f.execute(record, arena));
    }

    if (isTemporal6Param) {
        return execTemporalTemporal(vals);
    } else {
        return execTemporalStatic(vals);
    }
}

/* ─────────── helpers ─────────── */

VarVal
TemporalContainsGeometryPhysicalFunction::execTemporalStatic(const std::vector<VarVal>& p) const
{
    auto lon  = p[0].cast<nautilus::val<double>>();
    auto lat  = p[1].cast<nautilus::val<double>>();
    auto ts   = p[2].cast<nautilus::val<uint64_t>>();
    auto stat = p[3].cast<VariableSizedData>();

    std::cout << "4-param temporal-static contains function with coordinate values" << std::endl;

    const auto res = nautilus::invoke(
        +[](double lo, double la, uint64_t t, const char* g, uint32_t sz) -> int {
            try {
                MEOS::Meos::ensureMeosInitialized();
                if (!(lo >= -180.0 && lo <= 180.0 && la >= -90.0 && la <= 90.0)) {
                    std::cout << "TemporalContains: coordinates out of range" << std::endl;
                    return 0;
                }

                std::string tsStr = MEOS::Meos::convertEpochToTimestamp(t);
                std::string left  = fmt::format("SRID=4326;Point({} {})@{}", lo, la, tsStr);
                std::string right(g, sz);

                while (!right.empty() && (right.front() == '\'' || right.front() == '"'))
                    right = right.substr(1);
                while (!right.empty() && (right.back() == '\'' || right.back() == '"'))
                    right = right.substr(0, right.size() - 1);

                std::cout << "Built geometries:" << std::endl;
                std::cout << "Left (temporal): " << left << std::endl;
                std::cout << "Right (static): "  << right << std::endl;

                if (left.empty() || right.empty()) {
                    std::cout << "Empty geometry WKT string(s)" << std::endl;
                    return -1;
                }

                std::cout << "Using temporal-static contains function (contains_tgeo_geo)" << std::endl;

                MEOS::Meos::TemporalGeometry l(left);
                if (!l.getGeometry()) {
                    std::cout << "TemporalContains: MEOS temporal geometry is null" << std::endl;
                    return 0;
                }
                MEOS::Meos::StaticGeometry r(right);
                if (!r.getGeometry()) {
                    std::cout << "TemporalContains: MEOS static geometry is null" << std::endl;
                    return 0;
                }

                int contains_result = l.containsStatic(r);
                std::cout << "contains_tgeo_geo result: " << contains_result << std::endl;
                return contains_result;

            } catch (const std::exception& e) {
                std::cout << "MEOS exception in temporal geometry contains: " << e.what() << std::endl;
                return -1;
            } catch (...) {
                std::cout << "Unknown error in temporal geometry contains" << std::endl;
                return -1;
            }
        },
        lon, lat, ts, stat.getContent(), stat.getContentSize()
    );

    return VarVal(res);
}

VarVal
TemporalContainsGeometryPhysicalFunction::execTemporalTemporal(const std::vector<VarVal>& p) const
{
    auto lon1 = p[0].cast<nautilus::val<double>>();
    auto lat1 = p[1].cast<nautilus::val<double>>();
    auto ts1  = p[2].cast<nautilus::val<uint64_t>>();
    auto lon2 = p[3].cast<nautilus::val<double>>();
    auto lat2 = p[4].cast<nautilus::val<double>>();
    auto ts2  = p[5].cast<nautilus::val<uint64_t>>();

    std::cout << "6-param temporal-temporal contains function with coordinate values" << std::endl;

    const auto res = nautilus::invoke(
        +[](double lo1, double la1, uint64_t t1, double lo2, double la2, uint64_t t2) -> int {
            try {
                MEOS::Meos::ensureMeosInitialized();
                auto inRange = [](double lo, double la) {
                    return lo >= -180.0 && lo <= 180.0 && la >= -90.0 && la <= 90.0;
                };
                if (!inRange(lo1, la1) || !inRange(lo2, la2)) {
                    std::cout << "TemporalContains: coordinates out of range" << std::endl;
                    return 0;
                }

                auto tsToStr = MEOS::Meos::convertEpochToTimestamp;
                std::string left  = fmt::format("SRID=4326;Point({} {})@{}", lo1, la1, tsToStr(t1));
                std::string right = fmt::format("SRID=4326;Point({} {})@{}", lo2, la2, tsToStr(t2));

                std::cout << "Built geometries:" << std::endl;
                std::cout << "Left (temporal): "  << left  << std::endl;
                std::cout << "Right (temporal): " << right << std::endl;

                std::cout << "Using temporal-temporal contains function (contains_tgeo_tgeo)" << std::endl;

                MEOS::Meos::TemporalGeometry l(left), r(right);
                if (!l.getGeometry()) {
                    std::cout << "TemporalContains: MEOS left temporal geometry is null" << std::endl;
                    return 0;
                }
                if (!r.getGeometry()) {
                    std::cout << "TemporalContains: MEOS right temporal geometry is null" << std::endl;
                    return 0;
                }

                int contains_result = l.contains(r);
                std::cout << "contains_tgeo_tgeo result: " << contains_result << std::endl;
                return contains_result;

            } catch (const std::exception& e) {
                std::cout << "MEOS exception in temporal geometry contains: " << e.what() << std::endl;
                return -1;
            } catch (...) {
                std::cout << "Unknown error in temporal geometry contains" << std::endl;
                return -1;
            }
        },
        lon1, lat1, ts1, lon2, lat2, ts2
    );

    return VarVal(res);
}

/* ─────────── registry ─────────── */

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterTemporalContainsGeometryPhysicalFunction(
        PhysicalFunctionRegistryArguments physicalFunctionRegistryArguments)
{
    if (physicalFunctionRegistryArguments.childFunctions.size() == 6) {
        return TemporalContainsGeometryPhysicalFunction(
            physicalFunctionRegistryArguments.childFunctions[0],
            physicalFunctionRegistryArguments.childFunctions[1],
            physicalFunctionRegistryArguments.childFunctions[2],
            physicalFunctionRegistryArguments.childFunctions[3],
            physicalFunctionRegistryArguments.childFunctions[4],
            physicalFunctionRegistryArguments.childFunctions[5]
        );
    }
    PRECONDITION(physicalFunctionRegistryArguments.childFunctions.size() == 4,
                 "TemporalContainsGeometry expects 4 or 6 child functions, got {}",
                 physicalFunctionRegistryArguments.childFunctions.size());

    return TemporalContainsGeometryPhysicalFunction(
        physicalFunctionRegistryArguments.childFunctions[0],
        physicalFunctionRegistryArguments.childFunctions[1],
        physicalFunctionRegistryArguments.childFunctions[2],
        physicalFunctionRegistryArguments.childFunctions[3]
    );
}

} // namespace NES