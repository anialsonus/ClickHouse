#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/Transforms/JoiningTransform.h>
#include <Interpreters/ExpressionActions.h>
#include <IO/Operators.h>
#include <Interpreters/JoinSwitcher.h>
#include <Common/JSONBuilder.h>

#include <Common/logger_useful.h>

namespace DB
{

static ITransformingStep::Traits getTraits(const ActionsDAGPtr & actions, const Block & header, const SortDescription & sort_description)
{
    return ITransformingStep::Traits
    {
        {
            .preserves_distinct_columns = !actions->hasArrayJoin(),
            .returns_single_stream = false,
            .preserves_number_of_streams = true,
            .preserves_sorting = actions->isSortingPreserved(header, sort_description),
        },
        {
            .preserves_number_of_rows = !actions->hasArrayJoin(),
        }
    };
}

ExpressionStep::ExpressionStep(const DataStream & input_stream_, const ActionsDAGPtr & actions_dag_)
    : ITransformingStep(
        input_stream_,
        ExpressionTransform::transformHeader(input_stream_.header, *actions_dag_),
        getTraits(actions_dag_, input_stream_.header, input_stream_.sort_description))
    , actions_dag(actions_dag_)
{
    // LOG_TRACE(&Poco::Logger::get("ExpressionStep"), "ctor out_header structure {}, input_stream_ structure {}",
    //     ExpressionTransform::transformHeader(input_stream_.header, *actions_dag_).dumpStructure(), input_stream_.header.dumpStructure());
    LOG_TRACE(&Poco::Logger::get("ExpressionStep"), "ctor out_header structure {}, input_stream_ structure {}",
        output_stream->header.dumpStructure(), input_stream_.header.dumpStructure());
    /// Some columns may be removed by expression.
    updateDistinctColumns(output_stream->header, output_stream->distinct_columns);
}

// // !!!!!  deleted?
// void ExpressionStep::updateInputStream(DataStream input_stream, bool keep_header)
// {
//     Block out_header = keep_header ? std::move(output_stream->header)
//                                    : ExpressionTransform::transformHeader(input_stream.header, *actions_dag);
//     LOG_TRACE(&Poco::Logger::get("ExpressionStep"), "updateInputStream keep_header {}, out_header structure {}", keep_header, out_header.dumpStructure());
//     output_stream = createOutputStream(
//             input_stream,
//             std::move(out_header),
//             getDataStreamTraits());

//     input_streams.clear();
//     input_streams.emplace_back(std::move(input_stream));
// }

void ExpressionStep::transformPipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings)
{
    auto expression = std::make_shared<ExpressionActions>(actions_dag, settings.getActionsSettings());

    pipeline.addSimpleTransform([&](const Block & header)
    {
        return std::make_shared<ExpressionTransform>(header, expression);
    });

    if (!blocksHaveEqualStructure(pipeline.getHeader(), output_stream->header))
    {

        LOG_TRACE(&Poco::Logger::get("ExpressionStep"), "transformPipeline from {} to {}", pipeline.getHeader().dumpStructure(), output_stream->header.dumpStructure());
        assert(0);

        auto convert_actions_dag = ActionsDAG::makeConvertingActions(
                pipeline.getHeader().getColumnsWithTypeAndName(),
                output_stream->header.getColumnsWithTypeAndName(),
                ActionsDAG::MatchColumnsMode::Name);
        auto convert_actions = std::make_shared<ExpressionActions>(convert_actions_dag, settings.getActionsSettings());

        pipeline.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<ExpressionTransform>(header, convert_actions);
        });
    }
}

void ExpressionStep::describeActions(FormatSettings & settings) const
{
    String prefix(settings.offset, ' ');
    bool first = true;

    auto expression = std::make_shared<ExpressionActions>(actions_dag);
    for (const auto & action : expression->getActions())
    {
        settings.out << prefix << (first ? "Actions: "
                                         : "         ");
        first = false;
        settings.out << action.toString() << '\n';
    }

    settings.out << prefix << "Positions:";
    for (const auto & pos : expression->getResultPositions())
        settings.out << ' ' << pos;
    settings.out << '\n';
}

void ExpressionStep::describeActions(JSONBuilder::JSONMap & map) const
{
    auto expression = std::make_shared<ExpressionActions>(actions_dag);
    map.add("Expression", expression->toTree());
}

void ExpressionStep::updateOutputStream()
{
    output_stream = createOutputStream(
        input_streams.front(), ExpressionTransform::transformHeader(input_streams.front().header, *actions_dag), getDataStreamTraits());
}

}
