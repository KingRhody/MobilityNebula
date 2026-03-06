#pragma once

#include <Functions/LogicalFunction.hpp>
#include <string_view>
#include <utility>
#include <vector>

namespace NES {

class TemporalContainsGeometryLogicalFunction : public LogicalFunctionConcept {
public:
    static constexpr std::string_view NAME = "TemporalContainsGeometry";

    /// Constructor with 4 parameters: lon1, lat1, timestamp1, static_geometry_wkt
    TemporalContainsGeometryLogicalFunction(
        LogicalFunction lon1,
        LogicalFunction lat1,
        LogicalFunction timestamp1,
        LogicalFunction staticGeometry);

    /// Constructor with 6 parameters: lon1, lat1, timestamp1, lon2, lat2, timestamp2
    TemporalContainsGeometryLogicalFunction(
        LogicalFunction lon1,
        LogicalFunction lat1,
        LogicalFunction timestamp1,
        LogicalFunction lon2,
        LogicalFunction lat2,
        LogicalFunction timestamp2);

    /// Get the data type (always INT32 like Intersects)
    DataType getDataType() const override;

    /// Create new function with specified data type
    LogicalFunction withDataType(const DataType& dataType) const override;

    /// Get child functions
    std::vector<LogicalFunction> getChildren() const override;

    /// Replace children
    LogicalFunction withChildren(const std::vector<LogicalFunction>& children) const override;

    /// Get type name
    std::string_view getType() const override;

    /// Equality
    bool operator==(const LogicalFunctionConcept& rhs) const override;

    /// Explain
    std::string explain(ExplainVerbosity verbosity) const override;

    /// Type inference
    LogicalFunction withInferredDataType(const Schema& schema) const override;

    /// Serialization
    SerializableFunction serialize() const override;

private:
    DataType dataType;
    std::vector<LogicalFunction> parameters;
    bool isTemporal6Param;
};

} // namespace NES