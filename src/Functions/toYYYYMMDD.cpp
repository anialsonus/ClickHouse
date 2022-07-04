#include <Functions/FunctionFactory.h>
#include <Functions/DateTimeTransforms.h>
#include <Functions/FunctionDateOrDateTimeToSomething.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

using FunctionToYYYYMMDD = FunctionDateOrDateTimeToSomething<ToYYYYMMDDImpl/*, DataTypeUInt32*/>;

void registerFunctionToYYYYMMDD(FunctionFactory & factory)
{
    factory.registerFunction<FunctionToYYYYMMDD>();
}

}
