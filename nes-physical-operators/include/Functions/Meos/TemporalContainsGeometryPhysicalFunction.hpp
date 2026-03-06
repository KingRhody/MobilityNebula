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

#pragma once

#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <vector>

namespace NES {

class TemporalContainsGeometryPhysicalFunction : public PhysicalFunctionConcept {
public:
    /// Constructor for temporal–static (tgeo, geo): lon, lat, timestamp, static_geometry_wkt
    TemporalContainsGeometryPhysicalFunction(PhysicalFunction lonFunction,
                                             PhysicalFunction latFunction,
                                             PhysicalFunction timestampFunction,
                                             PhysicalFunction staticGeometryFunction);

    /// Constructor for temporal–temporal (tgeo, tgeo): lon1, lat1, timestamp1, lon2, lat2, timestamp2
    TemporalContainsGeometryPhysicalFunction(PhysicalFunction lon1Function,
                                             PhysicalFunction lat1Function,
                                             PhysicalFunction timestamp1Function,
                                             PhysicalFunction lon2Function,
                                             PhysicalFunction lat2Function,
                                             PhysicalFunction timestamp2Function);

    VarVal execute(const Record& record, ArenaRef& arena) const override;

private:
    std::vector<PhysicalFunction> paramFns;
    bool isTemporal6Param;  // true for temporal–temporal (6 params), false for temporal–static (4 params)

    // Helper methods for each case
    VarVal execTemporalStatic  (const std::vector<VarVal>& params) const;
    VarVal execTemporalTemporal(const std::vector<VarVal>& params) const;
};

} // namespace NES