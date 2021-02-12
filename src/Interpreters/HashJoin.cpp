#include <any>
#include <limits>

#include <common/logger_useful.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>

#include <Interpreters/HashJoin.h>
#include <Interpreters/join_common.h>
#include <Interpreters/TableJoin.h>
#include <Interpreters/joinDispatch.h>
#include <Interpreters/NullableUtils.h>
#include <Interpreters/DictionaryReader.h>

#include <Storages/StorageDictionary.h>

#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/materializeBlock.h>

#include <Core/ColumnNumbers.h>
#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
    extern const int INCOMPATIBLE_TYPE_OF_JOIN;
    extern const int UNSUPPORTED_JOIN_KEYS;
    extern const int LOGICAL_ERROR;
    extern const int SYNTAX_ERROR;
    extern const int SET_SIZE_LIMIT_EXCEEDED;
    extern const int TYPE_MISMATCH;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

struct NotProcessedCrossJoin : public ExtraBlock
{
    size_t left_position;
    size_t right_block;
};

}

namespace JoinStuff
{
    /// Version of `getUsed` with dynamic dispatch
    bool JoinUsedFlags::getUsedSafe(size_t i) const
    {
        if (flags.empty())
            return !need_flags;
        return flags[i].load();
    }

    template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS>
    void JoinUsedFlags::reinit(size_t size)
    {
        if constexpr (MapGetter<KIND, STRICTNESS>::flagged)
        {
            assert(flags.size() <= size);
            need_flags = true;
            flags = std::vector<std::atomic_bool>(size);
        }
    }

    template <>
    void JoinUsedFlags::setUsed<false>(size_t i [[maybe_unused]]) {}

    template <>
    bool JoinUsedFlags::getUsed<false>(size_t i [[maybe_unused]]) { return true; }

    template <>
    bool JoinUsedFlags::setUsedOnce<false>(size_t i [[maybe_unused]]) { return true; }

    template <>
    void JoinUsedFlags::setUsed<true>(size_t i)
    {
        /// Could be set simultaneously from different threads.
        flags[i].store(true, std::memory_order_relaxed);
    }

    template <>
    bool JoinUsedFlags::getUsed<true>(size_t i) { return flags[i].load(); }

    template <>
    bool JoinUsedFlags::setUsedOnce<true>(size_t i)
    {
        /// fast check to prevent heavy CAS with seq_cst order
        if (flags[i].load(std::memory_order_relaxed))
            return false;

        bool expected = false;
        return flags[i].compare_exchange_strong(expected, true);
    }
}

static ColumnPtr filterWithBlanks(ColumnPtr src_column, const IColumn::Filter & filter, bool inverse_filter = false)
{
    ColumnPtr column = src_column->convertToFullColumnIfConst();
    MutableColumnPtr mut_column = column->cloneEmpty();
    mut_column->reserve(column->size());

    if (inverse_filter)
    {
        for (size_t row = 0; row < filter.size(); ++row)
        {
            if (filter[row])
                mut_column->insertDefault();
            else
                mut_column->insertFrom(*column, row);
        }
    }
    else
    {
        for (size_t row = 0; row < filter.size(); ++row)
        {
            if (filter[row])
                mut_column->insertFrom(*column, row);
            else
                mut_column->insertDefault();
        }
    }

    return mut_column;
}

static ColumnWithTypeAndName correctNullability(ColumnWithTypeAndName && column, bool nullable)
{
    if (nullable)
    {
        JoinCommon::convertColumnToNullable(column);
    }
    else
    {
        /// We have to replace values masked by NULLs with defaults.
        if (column.column)
            if (const auto * nullable_column = checkAndGetColumn<ColumnNullable>(*column.column))
                column.column = filterWithBlanks(column.column, nullable_column->getNullMapColumn().getData(), true);

        JoinCommon::removeColumnNullability(column);
    }

    return std::move(column);
}

static ColumnWithTypeAndName correctNullability(ColumnWithTypeAndName && column, bool nullable, const ColumnUInt8 & negative_null_map)
{
    if (nullable)
    {
        JoinCommon::convertColumnToNullable(column, true);
        if (column.type->isNullable() && !negative_null_map.empty())
        {
            MutableColumnPtr mutable_column = IColumn::mutate(std::move(column.column));
            assert_cast<ColumnNullable &>(*mutable_column).applyNegatedNullMap(negative_null_map);
            column.column = std::move(mutable_column);
        }
    }
    else
        JoinCommon::removeColumnNullability(column);

    return std::move(column);
}


HashJoin::HashJoin(std::shared_ptr<TableJoin> table_join_, const Block & right_sample_block_, bool any_take_last_row_)
    : table_join(table_join_)
    , kind(table_join->kind())
    , strictness(table_join->strictness())
    , key_names_right(table_join->keyNamesRight())
    , key_names_left(table_join->keyNamesLeft())
    , nullable_right_side(table_join->forceNullableRight())
    , nullable_left_side(table_join->forceNullableLeft())
    , any_take_last_row(any_take_last_row_)
    , asof_inequality(table_join->getAsofInequality())
//    , data(std::make_shared<RightTableData>())
    , right_sample_block(right_sample_block_)
    , log(&Poco::Logger::get("HashJoin"))
{
    LOG_DEBUG(log, "HashJoin ctor Right sample block: {}", right_sample_block.dumpStructure());
    for (const auto & key_names_right_part : key_names_right)
    {
        for (const auto & aname : key_names_right_part)
            LOG_DEBUG(log, "key name right in ctor  {}", aname);
        LOG_DEBUG(log, ":");
    }
    for (const auto & key_names_left_part : key_names_left)
    {
        for (const auto & aname : key_names_left_part)
            LOG_DEBUG(log, "key name left in ctor  {}", aname);
        LOG_DEBUG(log, ":");
    }



    table_join->splitAdditionalColumns(right_sample_block, right_table_keys, sample_block_with_columns_to_add);
    required_right_keys = table_join->getRequiredRightKeys(right_table_keys, required_right_keys_sources);

    LOG_TRACE(log, "HashJoin: required_right_keys {} :", required_right_keys.dumpStructure());

    JoinCommon::removeLowCardinalityInplace(right_table_keys);
    data.resize(key_names_right.size());
    key_sizes.resize(key_names_right.size());

    Type join_method = Type::EMPTY;

    for (size_t i = 0; i < key_names_right.size(); ++i)
    {
        data[i] = std::make_shared<RightTableData>();
        initRightBlockStructure(data[i]->sample_block);
        LOG_TRACE(&Poco::Logger::get("HashJoin"), "ctor: sample_block {}  {}", i, data[i]->sample_block.dumpStructure());


        ColumnRawPtrs key_columns = JoinCommon::extractKeysForJoin(right_table_keys, key_names_right[i]);

        JoinCommon::createMissedColumns(sample_block_with_columns_to_add);
        if (nullable_right_side)
            JoinCommon::convertColumnsToNullable(sample_block_with_columns_to_add);

        if (table_join->dictionary_reader)
        {
            data[i]->type = join_method = Type::DICT;
            std::get<MapsOne>(data[i]->maps).create(Type::DICT);
            chooseMethod(key_columns, key_sizes[i]); /// init key_sizes
            // init(join_method, data[i]);
            continue;
        }
        else if (strictness == ASTTableJoin::Strictness::Asof)
        {
            /// @note ASOF JOIN is not INNER. It's better avoid use of 'INNER ASOF' combination in messages.
            /// In fact INNER means 'LEFT SEMI ASOF' while LEFT means 'LEFT OUTER ASOF'.
            if (!isLeft(kind) && !isInner(kind))
                throw Exception("Wrong ASOF JOIN type. Only ASOF and LEFT ASOF joins are supported", ErrorCodes::NOT_IMPLEMENTED);

            if (key_columns.size() <= 1)
                throw Exception("ASOF join needs at least one equi-join column", ErrorCodes::SYNTAX_ERROR);

            if (right_table_keys.getByName(key_names_right[0].back()).type->isNullable())
                throw Exception("ASOF join over right table Nullable column is not implemented", ErrorCodes::NOT_IMPLEMENTED);

            size_t asof_size;
            asof_type = AsofRowRefs::getTypeSize(*key_columns.back(), asof_size);
            key_columns.pop_back();

            /// this is going to set up the appropriate hash table for the direct lookup part of the join
            /// However, this does not depend on the size of the asof join key (as that goes into the BST)
            /// Therefore, add it back in such that it can be extracted appropriately from the full stored
            /// key_columns and key_sizes
            key_sizes[i].push_back(asof_size);
        }
        else
        {
            /// Choose data structure to use for JOIN.
        }

        auto current_join_method = chooseMethod(key_columns, key_sizes[i]);
        if (join_method == Type::EMPTY)
        {
            join_method = current_join_method;
        }
        else if (join_method != current_join_method)
        {
            join_method = Type::hashed;
        }
    }
    if (join_method != Type::DICT)
    {
        for (size_t i = 0; i < key_names_right.size(); ++i)
        {
            init(join_method, data[i]);
        }
    }

}

HashJoin::Type HashJoin::chooseMethod(const ColumnRawPtrs & key_columns, Sizes & key_sizes)
{
    size_t keys_size = key_columns.size();

    if (keys_size == 0)
        return Type::CROSS;

    bool all_fixed = true;
    size_t keys_bytes = 0;
    key_sizes.resize(keys_size);
    for (size_t j = 0; j < keys_size; ++j)
    {
        if (!key_columns[j]->isFixedAndContiguous())
        {
            all_fixed = false;
            break;
        }
        key_sizes[j] = key_columns[j]->sizeOfValueIfFixed();
        keys_bytes += key_sizes[j];
    }

    /// If there is one numeric key that fits in 64 bits
    if (keys_size == 1 && key_columns[0]->isNumeric())
    {
        size_t size_of_field = key_columns[0]->sizeOfValueIfFixed();
        if (size_of_field == 1)
            return Type::key8;
        if (size_of_field == 2)
            return Type::key16;
        if (size_of_field == 4)
            return Type::key32;
        if (size_of_field == 8)
            return Type::key64;
        if (size_of_field == 16)
            return Type::keys128;
        if (size_of_field == 32)
            return Type::keys256;
        throw Exception("Logical error: numeric column has sizeOfField not in 1, 2, 4, 8, 16, 32.", ErrorCodes::LOGICAL_ERROR);
    }

    /// If the keys fit in N bits, we will use a hash table for N-bit-packed keys
    if (all_fixed && keys_bytes <= 16)
        return Type::keys128;
    if (all_fixed && keys_bytes <= 32)
        return Type::keys256;

    /// If there is single string key, use hash table of it's values.
    if (keys_size == 1
        && (typeid_cast<const ColumnString *>(key_columns[0])
            || (isColumnConst(*key_columns[0]) && typeid_cast<const ColumnString *>(&assert_cast<const ColumnConst *>(key_columns[0])->getDataColumn()))))
        return Type::key_string;

    if (keys_size == 1 && typeid_cast<const ColumnFixedString *>(key_columns[0]))
        return Type::key_fixed_string;

    /// Otherwise, will use set of cryptographic hashes of unambiguously serialized values.
    return Type::hashed;
}

template<typename KeyGetter, bool is_asof_join>
static KeyGetter createKeyGetter(const ColumnRawPtrs & key_columns, const Sizes & key_sizes)
{
    if constexpr (is_asof_join)
    {
        auto key_column_copy = key_columns;
        auto key_size_copy = key_sizes;
        key_column_copy.pop_back();
        key_size_copy.pop_back();
        return KeyGetter(key_column_copy, key_size_copy, nullptr);
    }
    else
        return KeyGetter(key_columns, key_sizes, nullptr);
}

class KeyGetterForDict
{
public:
    using Mapped = RowRef;
    using FindResult = ColumnsHashing::columns_hashing_impl::FindResultImpl<Mapped, true>;

    KeyGetterForDict(const ColumnRawPtrs & key_columns_, const Sizes &, void *)
        : key_columns(key_columns_)
    {}

    FindResult findKey(const TableJoin & table_join, size_t row, const Arena &)
    {
        const DictionaryReader & reader = *table_join.dictionary_reader;
        if (!read_result)
        {
            reader.readKeys(*key_columns[0], read_result, found, positions);
            result.block = &read_result;

            LOG_TRACE(&Poco::Logger::get("HashJoin"), "KeyGetterForDict::findKey : result.block {}", result.block->dumpStructure());


            if (table_join.forceNullableRight())
                for (auto & column : read_result)
                    if (table_join.rightBecomeNullable(column.type))
                        JoinCommon::convertColumnToNullable(column);
        }

        result.row_num = positions[row];
        return FindResult(&result, found[row], 0);
    }

private:
    const ColumnRawPtrs & key_columns;
    Block read_result;
    Mapped result;
    ColumnVector<UInt8>::Container found;
    std::vector<size_t> positions;
};

template <HashJoin::Type type, typename Value, typename Mapped>
struct KeyGetterForTypeImpl;

constexpr bool use_offset = true;

template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::key8, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt8, false, use_offset>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::key16, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt16, false, use_offset>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::key32, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt32, false, use_offset>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::key64, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodOneNumber<Value, Mapped, UInt64, false, use_offset>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::key_string, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodString<Value, Mapped, true, false, use_offset>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::key_fixed_string, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodFixedString<Value, Mapped, true, false, use_offset>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::keys128, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodKeysFixed<Value, UInt128, Mapped, false, false, false, use_offset>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::keys256, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodKeysFixed<Value, DummyUInt256, Mapped, false, false, false, use_offset>;
};
template <typename Value, typename Mapped> struct KeyGetterForTypeImpl<HashJoin::Type::hashed, Value, Mapped>
{
    using Type = ColumnsHashing::HashMethodHashed<Value, Mapped, false, use_offset>;
};

template <HashJoin::Type type, typename Data>
struct KeyGetterForType
{
    // using Data = std::remove_reference_t<OrigData>;
    // using Data = std::decay_t<typename OrigData::value_type::type>;

    using Value = typename Data::value_type;
    using Mapped_t = typename Data::mapped_type;
    using Mapped = std::conditional_t<std::is_const_v<Data>, const Mapped_t, Mapped_t>;
    using Type = typename KeyGetterForTypeImpl<type, Value, Mapped>::Type;
};


void HashJoin::init(Type type_, RightTableDataPtr data_)
{
    data_->type = type_;

    if (kind == ASTTableJoin::Kind::Cross)
        return;
    joinDispatchInit(kind, strictness, data_->maps);
    joinDispatch(kind, strictness, data_->maps, [&](auto, auto, auto & map) { map.create(data_->type); });
}

bool HashJoin::overDictionary() const
{
    return data->type == Type::DICT;
}

bool HashJoin::empty() const
{
    return data->type == Type::EMPTY;
}

bool HashJoin::alwaysReturnsEmptySet() const
{
    return isInnerOrRight(getKind()) && data->empty && !overDictionary();
}

size_t HashJoin::getTotalRowCount() const
{
    size_t res = 0;

    for (const auto & data_part : data)
    {
        if (data_part->type == Type::CROSS)
        {
            for (const auto & block : data_part->blocks)
                res += block.rows();
        }
        else if (data_part->type != Type::DICT)
        {
            joinDispatch(kind, strictness, data_part->maps, [&](auto, auto, auto & map) { res += map.getTotalRowCount(data_part->type); });
        }
    }


    return res;
}

size_t HashJoin::getTotalByteCount() const
{
    size_t res = 0;

    for (const auto & data_part : data)
    {
        if (data_part->type == Type::CROSS)
        {
            for (const auto & block : data_part->blocks)
                res += block.bytes();
        }
        else if (data_part->type != Type::DICT)
        {
            joinDispatch(kind, strictness, data_part->maps, [&](auto, auto, auto & map) { res += map.getTotalByteCountImpl(data_part->type); });
            res += data_part->pool.size();
        }
    }


    return res;
}

namespace
{
    /// Inserting an element into a hash table of the form `key -> reference to a string`, which will then be used by JOIN.
    template <typename Map, typename KeyGetter>
    struct Inserter
    {
        static ALWAYS_INLINE void insertOne(const HashJoin & join, Map & map, KeyGetter & key_getter, Block * stored_block, size_t i,
                                            Arena & pool)
        {
            auto emplace_result = key_getter.emplaceKey(map, i, pool);

            if (emplace_result.isInserted() || join.anyTakeLastRow())
                new (&emplace_result.getMapped()) typename Map::mapped_type(stored_block, i);
        }

        static ALWAYS_INLINE void insertAll(const HashJoin &, Map & map, KeyGetter & key_getter, Block * stored_block, size_t i, Arena & pool)
        {
            auto emplace_result = key_getter.emplaceKey(map, i, pool);

            if (emplace_result.isInserted())
                new (&emplace_result.getMapped()) typename Map::mapped_type(stored_block, i);
            else
            {
                /// The first element of the list is stored in the value of the hash table, the rest in the pool.
                emplace_result.getMapped().insert({stored_block, i}, pool);
            }
        }

        static ALWAYS_INLINE void insertAsof(HashJoin & join, Map & map, KeyGetter & key_getter, Block * stored_block, size_t i, Arena & pool,
                                             const IColumn & asof_column)
        {
            auto emplace_result = key_getter.emplaceKey(map, i, pool);
            typename Map::mapped_type * time_series_map = &emplace_result.getMapped();

            TypeIndex asof_type = *join.getAsofType();
            if (emplace_result.isInserted())
                time_series_map = new (time_series_map) typename Map::mapped_type(asof_type);
            time_series_map->insert(asof_type, asof_column, stored_block, i);
        }
    };


    template <ASTTableJoin::Strictness STRICTNESS, typename KeyGetter, typename Map, bool has_null_map>
    size_t NO_INLINE insertFromBlockImplTypeCase(
        HashJoin & join, Map & map, size_t rows, const ColumnRawPtrs & key_columns,
        const Sizes & key_sizes, Block * stored_block, ConstNullMapPtr null_map, Arena & pool)
    {
        [[maybe_unused]] constexpr bool mapped_one = std::is_same_v<typename Map::mapped_type, RowRef>;
        constexpr bool is_asof_join = STRICTNESS == ASTTableJoin::Strictness::Asof;

        const IColumn * asof_column [[maybe_unused]] = nullptr;
        if constexpr (is_asof_join)
            asof_column = key_columns.back();

        auto key_getter = createKeyGetter<KeyGetter, is_asof_join>(key_columns, key_sizes);

        for (size_t i = 0; i < rows; ++i)
        {
            if (has_null_map && (*null_map)[i])
                continue;

            if constexpr (is_asof_join)
                Inserter<Map, KeyGetter>::insertAsof(join, map, key_getter, stored_block, i, pool, *asof_column);
            else if constexpr (mapped_one)
                Inserter<Map, KeyGetter>::insertOne(join, map, key_getter, stored_block, i, pool);
            else
                Inserter<Map, KeyGetter>::insertAll(join, map, key_getter, stored_block, i, pool);
        }
        return map.getBufferSizeInCells();
    }


    template <ASTTableJoin::Strictness STRICTNESS, typename KeyGetter, typename Map>
    size_t insertFromBlockImplType(
        HashJoin & join, Map & map, size_t rows, const ColumnRawPtrs & key_columns,
        const Sizes & key_sizes, Block * stored_block, ConstNullMapPtr null_map, Arena & pool)
    {
        if (null_map)
            return insertFromBlockImplTypeCase<STRICTNESS, KeyGetter, Map, true>(join, map, rows, key_columns, key_sizes, stored_block, null_map, pool);
        else
            return insertFromBlockImplTypeCase<STRICTNESS, KeyGetter, Map, false>(join, map, rows, key_columns, key_sizes, stored_block, null_map, pool);
    }


    template <ASTTableJoin::Strictness STRICTNESS, typename Maps>
    size_t insertFromBlockImpl(
        HashJoin & join, HashJoin::Type type, Maps & maps, size_t rows, const ColumnRawPtrs & key_columns,
        const Sizes & key_sizes, Block * stored_block, ConstNullMapPtr null_map, Arena & pool)
    {
        switch (type)
        {
            case HashJoin::Type::EMPTY: return 0;
            case HashJoin::Type::CROSS: return 0; /// Do nothing. We have already saved block, and it is enough.
            case HashJoin::Type::DICT:  return 0; /// No one should call it with Type::DICT.

        #define M(TYPE) \
            case HashJoin::Type::TYPE: \
                return insertFromBlockImplType<STRICTNESS, typename KeyGetterForType<HashJoin::Type::TYPE, std::remove_reference_t<decltype(*maps.TYPE)>>::Type>(\
                    join, *maps.TYPE, rows, key_columns, key_sizes, stored_block, null_map, pool); \
                    break;
            APPLY_FOR_JOIN_VARIANTS(M)
        #undef M
        }
        __builtin_unreachable();
    }
}

void HashJoin::initRightBlockStructure(Block & saved_block_sample)
{
    /// We could remove key columns for LEFT | INNER HashJoin but we should keep them for JoinSwitcher (if any).
    bool save_key_columns = !table_join->forceHashJoin() || isRightOrFull(kind) || key_names_right.size() > 1;
    if (save_key_columns)
    {
        saved_block_sample = right_table_keys.cloneEmpty();
    }
    else if (strictness == ASTTableJoin::Strictness::Asof)
    {
        /// Save ASOF key
        saved_block_sample.insert(right_table_keys.safeGetByPosition(right_table_keys.columns() - 1));
    }

    /// Save non key columns
    for (auto & column : sample_block_with_columns_to_add)
        saved_block_sample.insert(column);

    if (nullable_right_side)
        JoinCommon::convertColumnsToNullable(saved_block_sample, (isFull(kind) ? right_table_keys.columns() : 0));
}

Block HashJoin::structureRightBlock(const Block & block, size_t disjunct_num) const
{
    Block structured_block;
    for (const auto & sample_column : savedBlockSample(disjunct_num).getColumnsWithTypeAndName())
    {
        ColumnWithTypeAndName column = block.getByName(sample_column.name);
        if (sample_column.column->isNullable())
            JoinCommon::convertColumnToNullable(column);
        structured_block.insert(column);
    }

    return structured_block;
}

bool HashJoin::addJoinedBlock(const Block & source_block, bool check_limits)
{
    if (empty())
        throw Exception("Logical error: HashJoin was not initialized", ErrorCodes::LOGICAL_ERROR);
    if (overDictionary())
        throw Exception("Logical error: insert into hash-map in HashJoin over dictionary", ErrorCodes::LOGICAL_ERROR);

    LOG_TRACE(log, "addJoinedBlock: {}", source_block.dumpStructure());

    /// RowRef::SizeT is uint32_t (not size_t) for hash table Cell memory efficiency.
    /// It's possible to split bigger blocks and insert them by parts here. But it would be a dead code.
    if (unlikely(source_block.rows() > std::numeric_limits<RowRef::SizeT>::max()))
        throw Exception("Too many rows in right table block for HashJoin: " + toString(source_block.rows()), ErrorCodes::NOT_IMPLEMENTED);

    /// There's no optimization for right side const columns. Remove constness if any.
    Block block = materializeBlock(source_block);
    size_t rows = block.rows();

    size_t total_rows = 0;
    size_t total_bytes = 0;

    for (size_t p = 0; p < data.size(); ++p)
    {
        if (storage_join_lock.mutex())
            throw DB::Exception("addJoinedBlock called when HashJoin locked to prevent updates",
                                ErrorCodes::LOGICAL_ERROR);
        ColumnRawPtrs key_columns = JoinCommon::materializeColumnsInplace(block, key_names_right[p]);

        /// We will insert to the map only keys, where all components are not NULL.
        ConstNullMapPtr null_map{};
        ColumnPtr null_map_holder = extractNestedColumnsAndNullMap(key_columns, null_map);

        /// If RIGHT or FULL save blocks with nulls for NonJoinedBlockInputStream
        UInt8 save_nullmap = 0;
        if (isRightOrFull(kind) && null_map)
        {
            for (size_t i = 0; !save_nullmap && i < null_map->size(); ++i)
                save_nullmap |= (*null_map)[i];
        }

        Block structured_block = structureRightBlock(block, p);

        std::unique_lock lock(data[p]->rwlock);

        data[p]->blocks.emplace_back(std::move(structured_block));
        Block * stored_block = &data[p]->blocks.back();

        if (rows)
            data[p]->empty = false;

        if (kind != ASTTableJoin::Kind::Cross)
        {
            joinDispatch(kind, strictness, data[p]->maps, [&](auto kind_, auto strictness_, auto & map)
            {
                insertFromBlockImpl<strictness_>(*this, data[p]->type, map, rows, key_columns, key_sizes[p], stored_block, null_map, data[p]->pool);
                /// Number of buckets + 1 value from zero storage
                used_flags.reinit<kind_, strictness_>(size + 1);
            });
        }

        if (save_nullmap)
            data[p]->blocks_nullmaps.emplace_back(stored_block, null_map_holder);

        if (!check_limits)
            return true;

        /// TODO: Do not calculate them every time
        total_rows += getTotalRowCount();
        total_bytes += getTotalByteCount();
    }

    return table_join->sizeLimits().check(total_rows, total_bytes, "JOIN", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED);
}

using ColumnRawPtrsVector = std::vector<ColumnRawPtrs>;
using SizesVector = std::vector<Sizes>;

class AddedColumns
{
public:
    using TypeAndNames = std::vector<std::pair<decltype(ColumnWithTypeAndName::type), decltype(ColumnWithTypeAndName::name)>>;

    AddedColumns(const Block & block_with_columns_to_add, // left block
                 const Block & block, // AKA key_names_left
                 const Block & saved_block_sample, // AKA saved_block_with_columns_to_add
                 const HashJoin & join,
                 const ColumnRawPtrsVector & key_columns_,
                 const SizesVector & key_sizes_,
                 bool is_asof_join)
        : key_columns(key_columns_)
        , key_sizes(key_sizes_)
        , rows_to_add(block.rows())
        , asof_type(join.getAsofType())
        , asof_inequality(join.getAsofInequality())
    {
        size_t num_columns_to_add = block_with_columns_to_add.columns();
        if (is_asof_join)
            ++num_columns_to_add;

        columns.reserve(num_columns_to_add);
        type_name.reserve(num_columns_to_add);
        right_indexes.reserve(num_columns_to_add);

        for (const auto & src_column : block_with_columns_to_add)
        {
            /// Don't insert column if it's in left block
            if (!block.has(src_column.name))
                addColumn(src_column);
        }

        if (is_asof_join)
        {
            const ColumnWithTypeAndName & right_asof_column = join.rightAsofKeyColumn();
            addColumn(right_asof_column);
            left_asof_key = key_columns[0].back();   // !!!!
        }

        for (auto & tn : type_name)
            right_indexes.push_back(saved_block_sample.getPositionByName(tn.second));
    }

    size_t size() const { return columns.size(); }

    ColumnWithTypeAndName moveColumn(size_t i)
    {
        return ColumnWithTypeAndName(std::move(columns[i]), type_name[i].first, type_name[i].second);
    }

    template <bool has_defaults>
    void appendFromBlock(const Block & block, size_t row_num)
    {
        if constexpr (has_defaults)
            applyLazyDefaults();

        for (size_t j = 0, size = right_indexes.size(); j < size; ++j)
            columns[j]->insertFrom(*block.getByPosition(right_indexes[j]).column, row_num);
    }

    void appendDefaultRow()
    {
        ++lazy_defaults_count;
    }

    void applyLazyDefaults()
    {
        if (lazy_defaults_count)
        {
            for (size_t j = 0, size = right_indexes.size(); j < size; ++j)
                JoinCommon::addDefaultValues(*columns[j], type_name[j].first, lazy_defaults_count);
            lazy_defaults_count = 0;
        }
    }

    TypeIndex asofType() const { return *asof_type; }
    ASOF::Inequality asofInequality() const { return asof_inequality; }
    const IColumn & leftAsofKey() const { return *left_asof_key; }

public:
    const ColumnRawPtrsVector key_columns;
    const SizesVector key_sizes;


    size_t rows_to_add; // common
    std::unique_ptr<IColumn::Offsets> offsets_to_replicate; // common
    bool need_filter = false; // common
    IColumn::Filter row_filter; // common

private:
    TypeAndNames type_name;  // common - what we are about to add
    MutableColumns columns;  // common - what we are about to add
    std::vector<size_t> right_indexes; // common (because of type_name)

    size_t lazy_defaults_count = 0;
    /// for ASOF
    std::optional<TypeIndex> asof_type;
    ASOF::Inequality asof_inequality;
    const IColumn * left_asof_key = nullptr;

    void addColumn(const ColumnWithTypeAndName & src_column)
    {
        columns.push_back(src_column.column->cloneEmpty());
        columns.back()->reserve(src_column.column->size());
        type_name.emplace_back(src_column.type, src_column.name);
    }
};
using AddedColumnsV = std::vector<std::unique_ptr<AddedColumns>>;

namespace
{
template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS>
struct JoinFeatures
{
    static constexpr bool is_any_join = STRICTNESS == ASTTableJoin::Strictness::Any;
    static constexpr bool is_all_join = STRICTNESS == ASTTableJoin::Strictness::All;
    static constexpr bool is_asof_join = STRICTNESS == ASTTableJoin::Strictness::Asof;
    static constexpr bool is_semi_join = STRICTNESS == ASTTableJoin::Strictness::Semi;
    static constexpr bool is_anti_join = STRICTNESS == ASTTableJoin::Strictness::Anti;

    static constexpr bool left = KIND == ASTTableJoin::Kind::Left;
    static constexpr bool right = KIND == ASTTableJoin::Kind::Right;
    static constexpr bool inner = KIND == ASTTableJoin::Kind::Inner;
    static constexpr bool full = KIND == ASTTableJoin::Kind::Full;

    static constexpr bool need_replication = is_all_join || (is_any_join && right) || (is_semi_join && right);
    static constexpr bool need_filter = !need_replication && (inner || right || (is_semi_join && left) || (is_anti_join && left));
    static constexpr bool add_missing = (left || full) && !is_semi_join;
};





template <typename Map, bool add_missing>
void addFoundRowAll(const typename Map::mapped_type & mapped, AddedColumns & added, IColumn::Offset & current_offset)
{
    LOG_TRACE(&Poco::Logger::get("HashJoin"), "addFoundRowAll: add_missing {}", add_missing);
    if constexpr (add_missing)
        added.applyLazyDefaults();

    for (auto it = mapped.begin(); it.ok(); ++it)
    {
        LOG_TRACE(&Poco::Logger::get("HashJoin"), "addFoundRowAll: it->row_num {}, {}", it->row_num, it->block->dumpStructure());

        added.appendFromBlock<false>(*it->block, it->row_num);
        ++current_offset;
    }
};

template <typename Map, bool add_missing>
void addFoundRowAll(const typename Map::mapped_type & mapped, AddedColumns & added, IColumn::Offset & current_offset, std::unordered_set<IColumn::Offset> & known_rows)
{
    LOG_TRACE(&Poco::Logger::get("HashJoin"), "addFoundRowAll: add_missing {}", add_missing);
    if constexpr (add_missing)
        added.applyLazyDefaults();

    for (auto it = mapped.begin(); it.ok(); ++it)
    {
        LOG_TRACE(&Poco::Logger::get("HashJoin"), "addFoundRowAll: it->row_num {}, current_offset {}, {}", it->row_num, current_offset, it->block->dumpStructure());

        if (!known_rows.contains(it->row_num))
        {
            added.appendFromBlock<false>(*it->block, it->row_num);
            ++current_offset;
            known_rows.insert(it->row_num);
        }
    }
};

template <bool add_missing, bool need_offset>
void addNotFoundRow(AddedColumns & added [[maybe_unused]], IColumn::Offset & current_offset [[maybe_unused]])
{
    if constexpr (add_missing)
    {
        added.appendDefaultRow();
        if constexpr (need_offset)
            ++current_offset;
    }
}

template <bool need_filter>
void setUsed(IColumn::Filter & filter [[maybe_unused]], size_t pos [[maybe_unused]])
{
    if constexpr (need_filter)
        filter[pos] = 1;
}


/// Joins right table columns which indexes are present in right_indexes using specified map.
/// Makes filter (1 if row presented in right table) and returns offsets to replicate (for ALL JOINS).
template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename KeyGetter, typename Map, bool need_filter, bool has_null_map>
NO_INLINE IColumn::Filter joinRightColumns(
    const std::vector<const Map*> & mapv,
    AddedColumns & added_columns,
    const std::vector<ConstNullMapPtr> & null_map [[maybe_unused]],
    JoinStuff::JoinUsedFlags & used_flags [[maybe_unused]])
{
    JoinFeatures<KIND, STRICTNESS> jf;

    size_t rows = added_columns.rows_to_add;
    IColumn::Filter filter;
    if constexpr (need_filter)
        filter = IColumn::Filter(rows, 0);

    Arena pool; // a pool per disjunct?

    if constexpr (jf.need_replication)
        added_columns.offsets_to_replicate = std::make_unique<IColumn::Offsets>(rows);

    std::vector<KeyGetter> key_getter_vector;
    size_t disjunct_num = added_columns.key_columns.size();

    for (size_t i = 0; i < disjunct_num; ++i)
    {
        LOG_TRACE(&Poco::Logger::get("joinRightColumns"), "creating key_getter {}, {}", added_columns.key_columns[i].size(), added_columns.key_sizes[i].size());

        if (!added_columns.key_columns[i].empty())
        {
            LOG_TRACE(&Poco::Logger::get("joinRightColumns"), "creating key_getter column name {}", added_columns.key_columns[i][0]->getName());
        }

        auto key_getter = createKeyGetter<KeyGetter, jf.is_asof_join>(added_columns.key_columns[i], added_columns.key_sizes[i]);
        key_getter_vector.push_back(std::move(key_getter));
    }

    // auto key_getter = createKeyGetter<KeyGetter, jf.is_asof_join>(added_columns.key_columns, added_columns.key_sizes);

    IColumn::Offset current_offset = 0;


    for (size_t i = 0; i < rows; ++i)
    {
        std::unordered_set<IColumn::Offset> known_rows;
        for (size_t d = 0; d < disjunct_num; ++d)
        {
            if constexpr (has_null_map)
            {   // ????
                if (null_map[d] && (*null_map[d])[i] /* !null_map[d]->empty()*/)
                {
                    addNotFoundRow<jf.add_missing, jf.need_replication>(added_columns, current_offset);

                    if constexpr (jf.need_replication)
                    {
                        LOG_TRACE(&Poco::Logger::get("HashJoin"), "joinRightColumns: offsets_to_replicate (1) [{}] {}", i, current_offset);
                        (*added_columns.offsets_to_replicate)[i] = current_offset;
                    }

                    continue;
                }
            }

            auto find_result = key_getter_vector[d].findKey(*(mapv[d]), i, pool);

            if (find_result.isFound())
            {
                auto & mapped = find_result.getMapped();

                if constexpr (jf.is_asof_join)
                {
                    setUsed<need_filter>(filter, i);
                    used_flags.template setUsed<jf.need_flags>(find_result.getOffset());
                    added_columns.appendFromBlock<jf.add_missing>(*found->block, found->row_num);
                }
                else
                    addNotFoundRow<jf.add_missing, jf.need_replication>(added_columns, current_offset);
            }
            else if constexpr (jf.is_all_join)
            {
                setUsed<need_filter>(filter, i);
                used_flags.template setUsed<jf.need_flags>(find_result.getOffset());
                addFoundRowAll<Map, jf.add_missing>(mapped, added_columns, current_offset);
            }
            else if constexpr ((jf.is_any_join || jf.is_semi_join) && jf.right)
            {
                /// Use first appeared left key + it needs left columns replication
                bool used_once = used_flags.template setUsedOnce<need_flags>(find_result.getOffset());

                if (used_once)
                {
                    setUsed<need_filter>(filter, i);
                    mapped.setUsed();
                    addFoundRowAll<Map, jf.add_missing>(mapped, added_columns, current_offset, known_rows);
                }
            }
            else if constexpr (jf.is_any_join && KIND == ASTTableJoin::Kind::Inner)
            {
                bool used_once = used_flags.template setUsedOnce<need_flags>(find_result.getOffset());

                /// Use first appeared left key only
                if (used_once)
                {
                    setUsed<need_filter>(filter, i);
                    mapped.setUsed();
                    LOG_TRACE(&Poco::Logger::get("HashJoin"), "joinRightColumns: mapped.block {}", mapped.block->dumpStructure());
                    added_columns.appendFromBlock<jf.add_missing>(*mapped.block, mapped.row_num);
                }

                // break;

                if (jf.is_any_join)
                {
                    break;
                }
                else  // !!!!
                {
                }
            }
            else if constexpr (jf.is_any_join && jf.full)
            {
                /// TODO
            }
            else if constexpr (jf.is_anti_join)
            {
                if constexpr (jf.right && jf.need_flags)
                    used_flags.template setUsed<need_flags>(find_result.getOffset());
            }
            else /// ANY LEFT, SEMI LEFT, old ANY (RightAny)
            {
                setUsed<need_filter>(filter, i);
                used_flags.template setUsed<jf.need_flags>(find_result.getOffset());
                added_columns.appendFromBlock<jf.add_missing>(*mapped.block, mapped.row_num);
            else /// move out of the loop
            {
                if constexpr (jf.is_anti_join && jf.left)
                                 setUsed<need_filter>(filter, i);
                addNotFoundRow<jf.add_missing, jf.need_replication>(added_columns, current_offset);
            }
        }


        if constexpr (jf.need_replication)
        {
            LOG_TRACE(&Poco::Logger::get("HashJoin"), "joinRightColumns: offsets_to_replicate (2) [{}] {}", i, current_offset);
            (*added_columns.offsets_to_replicate)[i] = current_offset;
        }

    }

    added_columns.applyLazyDefaults();
    return filter;
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename KeyGetter, typename Map>
IColumn::Filter joinRightColumnsSwitchNullability(
    const std::vector<const Map*>/***/ & mapv, AddedColumns & added_columns, const std::vector<*/const !!!*/ConstNullMapPtr> & null_map, JoinStuff::JoinUsedFlags & used_flags)
{
    if (added_columns.need_filter)
    {
        if (!null_map.empty())
            return joinRightColumns<KIND, STRICTNESS, KeyGetter, Map, true, true>(mapv, added_columns, null_map, used_flags);
        else
            return joinRightColumns<KIND, STRICTNESS, KeyGetter, Map, true, false>(mapv, added_columns, nullptr, used_flags);
    }
    else
    {
        if (!null_map.empty())
            return joinRightColumns<KIND, STRICTNESS, KeyGetter, Map, false, true>(mapv, added_columns, null_map, used_flags);
        else
            return joinRightColumns<KIND, STRICTNESS, KeyGetter, Map, false, false>(mapv, added_columns, nullptr, used_flags);
    }
}


#if 0
template <typename Maps>
makeKeyGetter
    switch (type)
    {
    #define M(TYPE) \
        case HashJoin::Type::TYPE: \
            return joinRightColumnsSwitchNullability<KIND, STRICTNESS,\
                typename KeyGetterForType<HashJoin::Type::TYPE, const std::remove_reference_t<decltype(*maps_.TYPE)>>::Type>(\
                *maps_.TYPE, added_columns, null_map, used_flags);
        APPLY_FOR_JOIN_VARIANTS(M)
    #undef M

        default:
            throw Exception("Unsupported JOIN keys. Type: " + toString(static_cast<UInt32>(type)), ErrorCodes::UNSUPPORTED_JOIN_KEYS);
    }
#endif


template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Maps>
IColumn::Filter switchJoinRightColumns(const std::vector<const Maps*> & mapv, AddedColumns & added_columns, HashJoin::Type type, const std::vector<ConstNullMapPtr> & null_map)
{
    switch (type)
    {
            // using AMapType = decltype(std::declval<Maps>.TYPE);
            // using AMapType = typename std::remove_reference_t<decltype(Maps::TYPE)>::pointer;
            // std::vector<std::reference_wrapper<const AMapTypeVal>> a_map_type_vector;
            // using AMapTypeVal = typename std::remove_reference_t<decltype(Maps::TYPE)>::element_type;
            // using AMapTypeVal = typename std::remove_reference_t<decltype(std::declval<Maps::TYPE>().get())>::element_type;
    #define M(TYPE) \
        case HashJoin::Type::TYPE: \
            {                                                           \
            using AMapTypeVal = const typename std::remove_reference_t<decltype(Maps::TYPE)>::element_type; \
            std::vector<const AMapTypeVal*> a_map_type_vector;          \
            for (const auto el : mapv)                                  \
            {                                                           \
                a_map_type_vector.push_back(el->TYPE.get());            \
            }                                                           \
            return joinRightColumnsSwitchNullability<KIND, STRICTNESS,  \
                          typename KeyGetterForType<HashJoin::Type::TYPE, AMapTypeVal>::Type>( \
                a_map_type_vector, added_columns, null_map);\
    }

        APPLY_FOR_JOIN_VARIANTS(M)
    #undef M

        default:
            throw Exception("Unsupported JOIN keys in switchJoinRightColumns. Type: " + toString(static_cast<UInt32>(type)), ErrorCodes::UNSUPPORTED_JOIN_KEYS);
    }
}

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS>
IColumn::Filter dictionaryJoinRightColumns(const TableJoin & table_join, AddedColumns & added_columns, const ConstNullMapPtr & null_map)
{
    if constexpr (KIND == ASTTableJoin::Kind::Left &&
        (STRICTNESS == ASTTableJoin::Strictness::Any ||
        STRICTNESS == ASTTableJoin::Strictness::Semi ||
        STRICTNESS == ASTTableJoin::Strictness::Anti))
    {
        std::vector<const TableJoin*> maps_vector;
        maps_vector.push_back(&table_join);

        std::vector<ConstNullMapPtr> null_maps_vector;
        null_maps_vector.push_back(null_map);

        JoinStuff::JoinUsedFlags flags;
        return joinRightColumnsSwitchNullability<KIND, STRICTNESS, KeyGetterForDict>(maps_vector, added_columns, null_maps_vector, flags);
    }

    throw Exception("Logical error: wrong JOIN combination", ErrorCodes::LOGICAL_ERROR);
}

} /// nameless


#if 0
std::unique_ptr<AddedColumns> joinAddedColumns(
    AddedColumnsV & added_columns_v)
{
    auto first = std::move(added_columns_v.front());

    for (size_t i = 1; i < added_columns_v.size(); ++i)
    {
        if (!first->row_filter.empty())
        {
            assert(added_columns_v[i]->rows_to_add == first->row_filter.size());

            for (size_t r = 0; r < added_columns_v[i]->rows_to_add; ++r)
            {
                first->row_filter[r] = first->row_filter[r] && added_columns_v[i]->row_filter[r];
            }
        }

        if (!(*first->offsets_to_replicate).empty())
        {
            assert(added_columns_v[i]->rows_to_add == (*first->offsets_to_replicate).size());

            for (size_t r = 0; r < added_columns_v[i]->rows_to_add; ++r)
            {
                (*first->offsets_to_replicate)[r] += (*added_columns_v[i]->offsets_to_replicate)[r];
            }
        }
    }

    return first;   // avoid extra copy
}
#endif

template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Maps>
std::unique_ptr<AddedColumns> HashJoin::makeAddedColumns(
    Block & block,
    const NamesVector & key_names_left_vector,
    const Block & block_with_columns_to_add,
    const std::vector<const Maps*> & maps_,
    const SizesVector & key_sizes_,
    HashJoin::Type type_) const
{
    constexpr JoinFeatures<KIND, STRICTNESS> jf;

    /// Rare case, when keys are constant or low cardinality. To avoid code bloat, simply materialize them.

    ColumnRawPtrsVector left_key_columns_vector;
    std::vector<ConstNullMapPtr> null_map_vector;
    std::vector<ColumnPtr> null_map_holder_vector;
    std::vector<Columns> materialized_keys_vector;

    for (const auto & key_names_left_ : key_names_left_vector)
    {
        materialized_keys_vector.emplace_back(JoinCommon::materializeColumns(block, key_names_left_));
        ColumnRawPtrs left_key_columns = JoinCommon::getRawPointers(materialized_keys_vector.back());
        // emplace_back ?
        left_key_columns_vector.push_back(std::move(left_key_columns));

        null_map_vector.emplace_back();
        null_map_holder_vector.push_back(extractNestedColumnsAndNullMap(left_key_columns_vector.back(), null_map_vector.back()));
    }


    LOG_TRACE(&Poco::Logger::get("HashJoin"), "makeAddedColumns: block_with_columns_to_add {}", block_with_columns_to_add.dumpStructure());

    auto added_columns = std::make_unique<AddedColumns>(block_with_columns_to_add, block, savedBlockSample(0 /*!!*/), *this, left_key_columns_vector, key_sizes_, jf.is_asof_join);


    /// Keys with NULL value in any column won't join to anything.
    // ConstNullMapPtr null_map{};
    // ColumnPtr null_map_holder = extractNestedColumnsAndNullMap(left_key_columns, null_map);

    // size_t existing_columns = block.columns();

    // if constexpr (jf.right || jf.full)
    // {
    //     materializeBlockInplace(block);

    //     if (nullable_left_side)
    //         JoinCommon::convertColumnsToNullable(block);
    // }


    bool has_required_right_keys = (required_right_keys.columns() != 0);
    added_columns->need_filter = jf.need_filter || has_required_right_keys;

    LOG_TRACE(log, "makeAddedColumns: added_columns.rows_to_add {}, added_columns.size {}, has_required_right_keys {}, need_filter {}, need_replication {}, columns {}, rows {}", added_columns->rows_to_add, added_columns->size(), has_required_right_keys, jf.need_filter, jf.need_replication, block.columns(), block.rows());

    // for (const auto & a_column : added_columns->key_columns)
    // {
    //     LOG_TRACE(log, "joinBlockImpl: added_columns->key_column  {}", a_column->getName());
    // }

    added_columns->row_filter = overDictionary() ?
        dictionaryJoinRightColumns<KIND, STRICTNESS>(*table_join, *added_columns, null_map_vector[0]):
        switchJoinRightColumns<KIND, STRICTNESS>(maps_, *added_columns, type_, null_map_vector, used_flags);

    LOG_TRACE(log, "makeAddedColumns: block before insert {}", block.dumpStructure());
    for (size_t i = 0; i < added_columns->size(); ++i)
        block.insert(added_columns->moveColumn(i));

    LOG_TRACE(log, "makeAddedColumns: block after insert {}", block.dumpStructure());


    return added_columns;
}


template <ASTTableJoin::Kind KIND, ASTTableJoin::Strictness STRICTNESS, typename Maps>
void HashJoin::joinBlockImpl(
    Block & block,
    const Names & /*key_names_left_*/,
    const Block & /*block_with_columns_to_add*/,
    const Maps & /*maps_*/,
    const Sizes & /*key_sizes_*/,
    HashJoin::Type /*type_*/,
    std::unique_ptr<AddedColumns> added_columns,
    size_t existing_columns) const
{
    JoinFeatures<KIND, STRICTNESS> jf;
    // constexpr bool is_any_join = STRICTNESS == ASTTableJoin::Strictness::Any;
    // constexpr bool is_all_join = STRICTNESS == ASTTableJoin::Strictness::All;
    // constexpr bool is_asof_join = STRICTNESS == ASTTableJoin::Strictness::Asof;
    // constexpr bool is_semi_join = STRICTNESS == ASTTableJoin::Strictness::Semi;
    // constexpr bool is_anti_join = STRICTNESS == ASTTableJoin::Strictness::Anti;

    // constexpr bool left = KIND == ASTTableJoin::Kind::Left;
    // constexpr bool right = KIND == ASTTableJoin::Kind::Right;
    // constexpr bool inner = KIND == ASTTableJoin::Kind::Inner;
    // constexpr bool full = KIND == ASTTableJoin::Kind::Full;

    // constexpr bool need_replication = is_all_join || (is_any_join && right) || (is_semi_join && right);
    // constexpr bool need_filter = !need_replication && (inner || right || (is_semi_join && left) || (is_anti_join && left));

    /// Rare case, when keys are constant or low cardinality. To avoid code bloat, simply materialize them.
    // Columns materialized_keys = JoinCommon::materializeColumns(block, key_names_left_);
    // ColumnRawPtrs left_key_columns = JoinCommon::getRawPointers(materialized_keys);

    // /// Keys with NULL value in any column won't join to anything.
    // ConstNullMapPtr null_map{};
    // ColumnPtr null_map_holder = extractNestedColumnsAndNullMap(left_key_columns, null_map);

    // size_t existing_columns = block.columns();

    /** If you use FULL or RIGHT JOIN, then the columns from the "left" table must be materialized.
      * Because if they are constants, then in the "not joined" rows, they may have different values
      *  - default values, which can differ from the values of these constants.
      */
    if constexpr (jf.right || jf.full)
    {
        materializeBlockInplace(block);

        if (nullable_left_side)
            JoinCommon::convertColumnsToNullable(block);
    }

    /** For LEFT/INNER JOIN, the saved blocks do not contain keys.
      * For FULL/RIGHT JOIN, the saved blocks contain keys;
      *  but they will not be used at this stage of joining (and will be in `AdderNonJoined`), and they need to be skipped.
      * For ASOF, the last column is used as the ASOF column
      */

    // AddedColumns added_columns(block_with_columns_to_add, block, savedBlockSample(), *this, left_key_columns, key_sizes_, jf.is_asof_join);


    bool has_required_right_keys = (required_right_keys.columns() != 0);
    // added_columns.need_filter = jf.need_filter || has_required_right_keys;

    // LOG_TRACE(log, "joinBlockImpl: added_columns.rows_to_add {}, added_columns.size {}, has_required_right_keys {}, need_filter {}, need_replication {}, columns {}, rows {}", added_columns.rows_to_add, added_columns.size(), has_required_right_keys, jf.need_filter, jf.need_replication, block.columns(), block.rows());

    // for (const auto & a_column : added_columns.key_columns)
    // {
    //     LOG_TRACE(log, "joinBlockImpl: added_columns.key_column  {}", a_column->getName());
    // }



    // IColumn::Filter row_filter = overDictionary() ?
    //     dictionaryJoinRightColumns<KIND, STRICTNESS>(*table_join, added_columns, null_map) :
    //     switchJoinRightColumns<KIND, STRICTNESS>(maps_, added_columns, type_ /*data->type*/, null_map, used_flags);


    std::vector<size_t> right_keys_to_replicate [[maybe_unused]];

    LOG_TRACE(log, "block 1 {}",  block.dumpStructure());
    if constexpr (jf.need_filter)
    {
        /// If ANY INNER | RIGHT JOIN - filter all the columns except the new ones.
        for (size_t i = 0; i < existing_columns; ++i)
            block.safeGetByPosition(i).column = block.safeGetByPosition(i).column->filter(added_columns->row_filter, -1);

        LOG_TRACE(log, "joinBlockImpl: need_filter {}", block.dumpStructure());

        /// Add join key columns from right block if needed
        ///      using value from left table because of equality
        for (size_t i = 0; i < required_right_keys.columns(); ++i)
        {
            const auto & right_key = required_right_keys.getByPosition(i);
            if (!block.findByName(right_key.name))
            {
                const auto & left_name = required_right_keys_sources[i];


                const auto & col = block.getByName(left_name);
                bool is_nullable = nullable_right_side || right_key.type->isNullable();
                block.insert(correctNullability({col.column, col.type, right_key.name}, is_nullable));

                // is it guaranteed that left_name in existing columns ?
            }

            /// asof column is already in block.
            if (jf.is_asof_join && right_key.name == key_names_right.back())
                continue;

            const auto & col = block.getByName(left_name);
            bool is_nullable = nullable_right_side || right_key.type->isNullable();
            block.insert(correctNullability({col.column, col.type, right_key.name}, is_nullable));
        }
    }
    else if (has_required_right_keys)
    {
        /// Some trash to represent IColumn::Filter as ColumnUInt8 needed for ColumnNullable::applyNullMap()
        auto null_map_filter_ptr = ColumnUInt8::create();
        ColumnUInt8 & null_map_filter = assert_cast<ColumnUInt8 &>(*null_map_filter_ptr);
        null_map_filter.getData().swap(added_columns->row_filter);
        const IColumn::Filter & filter = null_map_filter.getData();
        LOG_TRACE(log, "joinBlockImpl: null_map_filter");

        /// Add join key columns from right block if needed.
        for (size_t i = 0; i < required_right_keys.columns(); ++i)
        {
            const auto & right_key = required_right_keys.getByPosition(i);
            if (!block.findByName(right_key.name))
            {
                LOG_TRACE(log, "joinBlockImpl: adding required right key {}", right_key.name);
                const auto & left_name = required_right_keys_sources[i];

                /// asof column is already in block.
                if (jf.is_asof_join && right_key.name == key_names_right.back())
                    continue;

                const auto & col = block.getByName(left_name);
                bool is_nullable = nullable_right_side || right_key.type->isNullable();

                ColumnPtr thin_column = filterWithBlanks(col.column, filter);
                block.insert(correctNullability({thin_column, col.type, right_key.name}, is_nullable, null_map_filter));

                if constexpr (jf.need_replication)
                    right_keys_to_replicate.push_back(block.getPositionByName(right_key.name));
            }
            else
            {
                LOG_TRACE(log, "joinBlockImpl: skipping required right key {} (already added)", right_key.name);
            }
        }

    }

    LOG_TRACE(log, "block 2 {}",  block.dumpStructure());
    if constexpr (jf.need_replication)
    {
        std::unique_ptr<IColumn::Offsets> & offsets_to_replicate = added_columns->offsets_to_replicate;

        LOG_TRACE(log, "joinBlockImpl: offsets_to_replicate->size() 1 {}, existing columns {}", offsets_to_replicate->size(), existing_columns);

        /// If ALL ... JOIN - we replicate all the columns except the new ones.
        for (size_t i = 0; i < existing_columns; ++i)
            block.safeGetByPosition(i).column = block.safeGetByPosition(i).column->replicate(*offsets_to_replicate);
        LOG_TRACE(log, "joinBlockImpl: offsets_to_replicate->size() 2 {}, block {}", offsets_to_replicate->size(), block.dumpStructure());

        /// Replicate additional right keys
        for (size_t pos : right_keys_to_replicate)
        {
            LOG_TRACE(log, "joinBlockImpl: column->size() before {}, pos {}, block {}", block.safeGetByPosition(pos).column->size(), pos, block.dumpStructure());

            block.safeGetByPosition(pos).column = block.safeGetByPosition(pos).column->replicate(*offsets_to_replicate);
            LOG_TRACE(log, "joinBlockImpl: column->size() after {}", block.safeGetByPosition(pos).column->size());
        }

    }
}

void HashJoin::joinBlockImplCross(Block & block, ExtraBlockPtr & not_processed) const
{
    LOG_TRACE(log, "joinBlockImplCross: {}", block.dumpStructure());

    size_t max_joined_block_rows = table_join->maxJoinedBlockRows();
    size_t start_left_row = 0;
    size_t start_right_block = 0;

    if (not_processed)
    {
        auto & continuation = static_cast<NotProcessedCrossJoin &>(*not_processed);
        start_left_row = continuation.left_position;
        start_right_block = continuation.right_block;
        not_processed.reset();
    }

    size_t num_existing_columns = block.columns();
    size_t num_columns_to_add = sample_block_with_columns_to_add.columns();

    ColumnRawPtrs src_left_columns;
    MutableColumns dst_columns;

    {
        src_left_columns.reserve(num_existing_columns);
        dst_columns.reserve(num_existing_columns + num_columns_to_add);

        for (const ColumnWithTypeAndName & left_column : block)
        {
            src_left_columns.push_back(left_column.column.get());
            dst_columns.emplace_back(src_left_columns.back()->cloneEmpty());
        }

        for (const ColumnWithTypeAndName & right_column : sample_block_with_columns_to_add)
            dst_columns.emplace_back(right_column.column->cloneEmpty());

        for (auto & dst : dst_columns)
            dst->reserve(max_joined_block_rows);
    }

    size_t rows_left = block.rows();
    size_t rows_added = 0;

    for (size_t left_row = start_left_row; left_row < rows_left; ++left_row)
    {
        size_t block_number = 0;
        for (const Block & block_right : data[0]->blocks)
        {
            ++block_number;
            if (block_number < start_right_block)
                continue;

            size_t rows_right = block_right.rows();
            rows_added += rows_right;

            for (size_t col_num = 0; col_num < num_existing_columns; ++col_num)
                dst_columns[col_num]->insertManyFrom(*src_left_columns[col_num], left_row, rows_right);

            for (size_t col_num = 0; col_num < num_columns_to_add; ++col_num)
            {
                const IColumn & column_right = *block_right.getByPosition(col_num).column;
                dst_columns[num_existing_columns + col_num]->insertRangeFrom(column_right, 0, rows_right);
            }
        }

        start_right_block = 0;

        if (rows_added > max_joined_block_rows)
        {
            not_processed = std::make_shared<NotProcessedCrossJoin>(
                NotProcessedCrossJoin{{block.cloneEmpty()}, left_row, block_number + 1});
            not_processed->block.swap(block);
            break;
        }
    }

    for (const ColumnWithTypeAndName & src_column : sample_block_with_columns_to_add)
        block.insert(src_column);

    block = block.cloneWithColumns(std::move(dst_columns));
}

DataTypePtr HashJoin::joinGetCheckAndGetReturnType(const DataTypes & data_types, const String & column_name, bool or_null) const
{
    // std::shared_lock lock(data[0]->rwlock);

    size_t num_keys = data_types.size();
    if (right_table_keys.columns() != num_keys)
        throw Exception(
            "Number of arguments for function joinGet" + toString(or_null ? "OrNull" : "")
                + " doesn't match: passed, should be equal to " + toString(num_keys),
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    for (size_t i = 0; i < num_keys; ++i)
    {
        const auto & left_type_origin = data_types[i];
        const auto & [c2, right_type_origin, right_name] = right_table_keys.safeGetByPosition(i);
        auto left_type = removeNullable(recursiveRemoveLowCardinality(left_type_origin));
        auto right_type = removeNullable(recursiveRemoveLowCardinality(right_type_origin));
        if (!left_type->equals(*right_type))
            throw Exception(
                "Type mismatch in joinGet key " + toString(i) + ": found type " + left_type->getName() + ", while the needed type is "
                    + right_type->getName(),
                ErrorCodes::TYPE_MISMATCH);
    }

    if (!sample_block_with_columns_to_add.has(column_name))
        throw Exception("StorageJoin doesn't contain column " + column_name, ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);

    auto elem = sample_block_with_columns_to_add.getByName(column_name);
    if (or_null)
        elem.type = makeNullable(elem.type);
    return elem.type;
}


// TODO: return multiple columns as named tuple
// TODO: return array of values when strictness == ASTTableJoin::Strictness::All
ColumnWithTypeAndName HashJoin::joinGet(const Block & block, const Block & block_with_columns_to_add) const
{
    bool is_valid = (strictness == ASTTableJoin::Strictness::Any || strictness == ASTTableJoin::Strictness::RightAny)
        && kind == ASTTableJoin::Kind::Left;
    if (!is_valid)
        throw Exception("joinGet only supports StorageJoin of type Left Any", ErrorCodes::INCOMPATIBLE_TYPE_OF_JOIN);

    /// Assemble the key block with correct names.
    Block keys;
    for (size_t i = 0; i < block.columns(); ++i)
    {
        auto key = block.getByPosition(i);
        key.name = key_names_right[i];
        keys.insert(std::move(key));
    }

    static_assert(!MapGetter<ASTTableJoin::Kind::Left, ASTTableJoin::Strictness::Any>::flagged,
                  "joinGet are not protected from hash table changes between block processing");


    size_t existing_columns = block.columns();

    std::vector<const Maps*> maps_vector;
    maps_vector.push_back(&maps_);

    auto added_columns = makeAddedColumns<ASTTableJoin::Kind::Left, ASTTableJoin::Strictness::Any>(
        keys, key_names_right/*[0]*/, block_with_columns_to_add, maps_vector, key_sizes/*[0]*/, type_);


    joinBlockImpl<ASTTableJoin::Kind::Left, ASTTableJoin::Strictness::Any>(
        keys, key_names_right[0], block_with_columns_to_add, maps_, std::move(added_columns));


    return keys.getByPosition(keys.columns() - 1);}
}


void HashJoin::joinBlock(Block & block, ExtraBlockPtr & not_processed)
{
    LOG_TRACE(log, "joinBlock: left {}, right {}",
        block.dumpStructure(), right_table_keys.dumpStructure());

    if (kind == ASTTableJoin::Kind::Cross)
    {
        joinBlockImplCross(block, not_processed);
        return;
    }
    else if (kind == ASTTableJoin::Kind::Right || kind == ASTTableJoin::Kind::Full)
    {
        materializeBlockInplace(block);

        if (nullable_left_side)
            JoinCommon::convertColumnsToNullable(block);
    }

    AddedColumnsV added_columns_v;
    size_t existing_columns = block.columns();



    // std::unique_ptr<AddedColumns> added_columns = std::move(added_columns_v.back());

    if (overDictionary())
    {
        using Kind = ASTTableJoin::Kind;
        using Strictness = ASTTableJoin::Strictness;

        auto & map = std::get<MapsOne>(data[0]->maps);
        std::vector<const std::decay_t<decltype(map)>*> maps_vector;
        maps_vector.push_back(&map);

        // auto added_columns = makeAddedColumns<ASTTableJoin::Kind::Left, ASTTableJoin::Strictness::Any>(
        //     block, key_names_left/*[0]*/, sample_block_with_columns_to_add, maps_vector, key_sizes/*[0]*/, data[0]->type);

        if (kind == Kind::Left)
        {
            switch (strictness)
            {
            case Strictness::Any:
            case Strictness::All:
                {
                    auto added_columns = makeAddedColumns<Kind::Left, Strictness::Any>(
                        block, key_names_left/*[0]*/, sample_block_with_columns_to_add, maps_vector, key_sizes/*[0]*/, data[0]->type);
                    joinBlockImpl<Kind::Left, Strictness::Any>(block, key_names_left[0], sample_block_with_columns_to_add, map, key_sizes[0], data[0]->type, std::move(added_columns), existing_columns);
                    break;
                }

            case Strictness::Semi:
                {
                    auto added_columns = makeAddedColumns<Kind::Left, Strictness::Semi>(
                        block, key_names_left/*[0]*/, sample_block_with_columns_to_add, maps_vector, key_sizes/*[0]*/, data[0]->type);
                    joinBlockImpl<Kind::Left, Strictness::Semi>(block, key_names_left[0], sample_block_with_columns_to_add, map, key_sizes[0], data[0]->type, std::move(added_columns), existing_columns);
                    break;
                }
            case Strictness::Anti:
                {
                    auto added_columns = makeAddedColumns<Kind::Left, Strictness::Anti>(
                        block, key_names_left/*[0]*/, sample_block_with_columns_to_add, maps_vector, key_sizes/*[0]*/, data[0]->type);
                    joinBlockImpl<Kind::Left, Strictness::Anti>(block, key_names_left[0], sample_block_with_columns_to_add, map, key_sizes[0], data[0]->type, std::move(added_columns), existing_columns);
                    break;
                }
            default:
                throw Exception("Logical error: wrong JOIN combination", ErrorCodes::LOGICAL_ERROR);
            }
        }
        else if (kind == Kind::Inner && strictness == Strictness::All)
        {
            auto added_columns = makeAddedColumns<Kind::Left, Strictness::Semi>(
                block, key_names_left/*[0]*/, sample_block_with_columns_to_add, maps_vector, key_sizes/*[0]*/, data[0]->type);
            joinBlockImpl<Kind::Left, Strictness::Semi>(block, key_names_left[0], sample_block_with_columns_to_add, map, key_sizes[0], data[0]->type, std::move(added_columns), existing_columns);
        }

        else
            throw Exception("Logical error: wrong JOIN combination", ErrorCodes::LOGICAL_ERROR);
    }
    else
    {

        // MapsVariantPtrVector maps_vector;
        std::vector<const std::decay_t<decltype(data[0]->maps)>* > maps_vector;


        for (size_t i = 0; i < key_names_left.size(); ++i)
        {
            std::shared_lock lock(data[i]->rwlock);  // !!!!

            // const Names & key_names_left = table_join->keyNamesLeft();

            // for (auto & aname : key_names_left)
            //   LOG_TRACE(log, "key_name_left {}", aname);
            // for (auto & aname : key_names_right)
            //   LOG_TRACE(log, "key_name_right {}", aname);


            JoinCommon::checkTypesOfKeys(block, key_names_left[i], right_table_keys, key_names_right[i]);
            maps_vector.push_back(&data[i]->maps);
        }
        std::unique_ptr<AddedColumns> added_columns;

        joinDispatch(kind, strictness, maps_vector, [&](auto kind_, auto strictness_, auto & maps_vector_)
                                                    {
                                                        added_columns = makeAddedColumns<kind_, strictness_>(block, key_names_left, sample_block_with_columns_to_add, maps_vector_, key_sizes, data[0]->type/*one for all*/);
                                                    });

        if (joinDispatch(kind, strictness, data[0]->maps, [&](auto kind_, auto strictness_, auto & map)
                                               {
                                                   joinBlockImpl<kind_, strictness_>(block, key_names_left[0], sample_block_with_columns_to_add, map, key_sizes[0], data[0]->type, std::move(added_columns), existing_columns);
                                               }))
        {
            /// Joined
        }
        else
            throw Exception("Logical error: unknown combination of JOIN", ErrorCodes::LOGICAL_ERROR);
    }


    auto rows_num = block.rows();
    auto cols_num = block.columns();

    LOG_TRACE(log, "joinBlock end of iter, structure :  {}, num of rows: {}, num of columns: {} ",
        block.dumpStructure(), rows_num, cols_num);

    // for (size_t col_num = 0; col_num < cols_num; ++col_num)
    // {
    //     for (size_t row_num = 0; row_num < rows_num; ++row_num)
    //     {
    //         auto val = block.safeGetByPosition(col_num).column->getInt(row_num);
    //         LOG_TRACE(log, "joinBlock {} {} : {}",
    //             col_num, row_num, val);
    //     }
    // }
}


void HashJoin::joinTotals(Block & block) const
{
    JoinCommon::joinTotals(totals, sample_block_with_columns_to_add, *table_join, block);
    // for (const auto & key_names_right_part : key_names_right)    /* ???? */
    // {
    //     JoinCommon::joinTotals(totals, sample_block_with_columns_to_add, key_names_right_part, block);
    // }
}


template <typename Mapped>
struct AdderNonJoined
{
    static void add(const Mapped & mapped, size_t & rows_added, MutableColumns & columns_right)
    {
        constexpr bool mapped_asof = std::is_same_v<Mapped, AsofRowRefs>;
        [[maybe_unused]] constexpr bool mapped_one = std::is_same_v<Mapped, RowRef>;

        if constexpr (mapped_asof)
        {
            /// Do nothing
        }
        else if constexpr (mapped_one)
        {
            for (size_t j = 0; j < columns_right.size(); ++j)
            {
                const auto & mapped_column = mapped.block->getByPosition(j).column;
                columns_right[j]->insertFrom(*mapped_column, mapped.row_num);
            }

            ++rows_added;
        }
        else
        {
            for (auto it = mapped.begin(); it.ok(); ++it)
            {
                for (size_t j = 0; j < columns_right.size(); ++j)
                {
                    const auto & mapped_column = it->block->getByPosition(j).column;
                    columns_right[j]->insertFrom(*mapped_column, it->row_num);
                }

                ++rows_added;
            }
        }
    }
};


/// Stream from not joined earlier rows of the right table.
class NonJoinedBlockInputStream : private NotJoined, public IBlockInputStream
{
public:
    NonJoinedBlockInputStream(const HashJoin & parent_, const Block & result_sample_block_, UInt64 max_block_size_)
        : NotJoined(*parent_.table_join,
                    parent_.savedBlockSample(0 /*!!!!*/),
                    parent_.right_sample_block,
                    result_sample_block_)
        , parent(parent_)
        , max_block_size(max_block_size_)
    {}

    String getName() const override { return "NonJoined"; }
    Block getHeader() const override { return result_sample_block; }

protected:
    Block readImpl() override
    {
        if (parent.data[0]->blocks.empty())
            return Block();
        return createBlock();
    }

private:
    const HashJoin & parent;
    UInt64 max_block_size;

    std::any position;
    std::optional<HashJoin::BlockNullmapList::const_iterator> nulls_position;

    Block createBlock()
    {
        MutableColumns columns_right = saved_block_sample.cloneEmptyColumns();

        size_t rows_added = 0;

        auto fill_callback = [&](auto, auto strictness, auto & map)
        {
            rows_added = fillColumnsFromMap<strictness>(map, columns_right);
        };

        if (!joinDispatch(parent.kind, parent.strictness, parent.data[0]->maps, fill_callback))
            throw Exception("Logical error: unknown JOIN strictness (must be on of: ANY, ALL, ASOF)", ErrorCodes::LOGICAL_ERROR);

        fillNullsFromBlocks(columns_right, rows_added);
        if (!rows_added)
            return {};

        correctLowcardAndNullability(columns_right);

        Block res = result_sample_block.cloneEmpty();
        addLeftColumns(res, rows_added);
        addRightColumns(res, columns_right);
        copySameKeys(res);
        return res;
    }

    template <ASTTableJoin::Strictness STRICTNESS, typename Maps>
    size_t fillColumnsFromMap(const Maps & maps, MutableColumns & columns_keys_and_right /*, HashJoin::Type type*/)
    {
        switch (parent.data[0]->type)
        {
        #define M(TYPE) \
            case HashJoin::Type::TYPE: \
                return fillColumns<STRICTNESS>(*maps.TYPE, columns_keys_and_right);
            APPLY_FOR_JOIN_VARIANTS(M)
        #undef M
            default:
                throw Exception("Unsupported JOIN keys in fillColumnsFromMap. Type: " + toString(static_cast<UInt32>(parent.data[0]->type)),
                                ErrorCodes::UNSUPPORTED_JOIN_KEYS);
        }

        __builtin_unreachable();
    }

    template <ASTTableJoin::Strictness STRICTNESS, typename Map>
    size_t fillColumns(const Map & map, MutableColumns & columns_keys_and_right)
    {
        using Mapped = typename Map::mapped_type;
        using Iterator = typename Map::const_iterator;

        size_t rows_added = 0;

        if (!position.has_value())
            position = std::make_any<Iterator>(map.begin());

        Iterator & it = std::any_cast<Iterator &>(position);
        auto end = map.end();

        for (; it != end; ++it)
        {
            const Mapped & mapped = it->getMapped();

            size_t off = map.offsetInternal(it.getPtr());
            if (parent.isUsed(off))
                continue;

            AdderNonJoined<Mapped>::add(mapped, rows_added, columns_keys_and_right);

            if (rows_added >= max_block_size)
            {
                ++it;
                break;
            }
        }

        return rows_added;
    }

    void fillNullsFromBlocks(MutableColumns & columns_keys_and_right, size_t & rows_added)
    {
        if (!nulls_position.has_value())
            nulls_position = parent.data[0]->blocks_nullmaps.begin();

        auto end = parent.data[0]->blocks_nullmaps.end();

        for (auto & it = *nulls_position; it != end && rows_added < max_block_size; ++it)
        {
            const Block * block = it->first;
            const NullMap & nullmap = assert_cast<const ColumnUInt8 &>(*it->second).getData();

            for (size_t row = 0; row < nullmap.size(); ++row)
            {
                if (nullmap[row])
                {
                    for (size_t col = 0; col < columns_keys_and_right.size(); ++col)
                        columns_keys_and_right[col]->insertFrom(*block->getByPosition(col).column, row);
                    ++rows_added;
                }
            }
        }
    }
};


BlockInputStreamPtr HashJoin::createStreamWithNonJoinedRows(const Block & result_sample_block, UInt64 max_block_size) const
{
    if (table_join->strictness() == ASTTableJoin::Strictness::Asof ||
        table_join->strictness() == ASTTableJoin::Strictness::Semi)
        return {};

    if (isRightOrFull(table_join->kind()))
        return std::make_shared<NonJoinedBlockInputStream>(*this, result_sample_block, max_block_size);
    return {};
}

void HashJoin::reuseJoinedData(const HashJoin & join)
{
    data = join.data;
    joinDispatch(kind, strictness, data->maps, [this](auto kind_, auto strictness_, auto & map)
    {
        used_flags.reinit<kind_, strictness_>(map.getBufferSizeInCells(data->type) + 1);
    });
}


#if 0

//////////////////////////
HashJoin::HashJoin(std::shared_ptr<TableJoin> table_join_, const Block & right_sample_block)
    //        : impl(table_join_, right_sample_block)
    : log(&Poco::Logger::get("HashJoin"))
    , keyNamesLeft(table_join_->key_names_left)
    {
        size_t num_keys = table_join_->key_names_left.size();
        for (size_t i = 0; i < num_keys; ++i)
        {
            Names key_names_left_orig = table_join_->key_names_left;
            table_join_->key_names_left.clear();
            table_join_->key_names_left.push_back(key_names_left_orig[i]);


            Names key_names_right_orig = table_join_->key_names_right;
            table_join_->key_names_right.clear();
            table_join_->key_names_right.push_back(key_names_right_orig[i]);


            implv.push_back(std::make_unique<HashJoin>(table_join_, right_sample_block));

            table_join_->key_names_left = key_names_left_orig;
            table_join_->key_names_right = key_names_right_orig;
        }

    }


Block HashJoin::mergeBlocks(Blocks && blocks, size_t num_rows) const
{
    // size_t num_rows = 0;

    // { /// Merge sort blocks
    //     BlockInputStreams inputs;
    //     inputs.reserve(blocks.size());

    //     for (auto & block : blocks)
    //     {
    //         num_rows += block.rows();
    //         inputs.emplace_back(std::make_shared<OneBlockInputStream>(block));
    //     }

    //     Blocks tmp_blocks;
    //     SortDescription sort_description;
    //     for (const auto & key_name : keyNamesLeft)
    //     {
    //         sort_description.emplace_back(SortColumnDescription(key_name, 1, 1));
    //     }

    //     MergingSortedBlockInputStream stream(inputs, sort_description, num_rows);
    //     while (const auto & block = stream.read())
    //         tmp_blocks.emplace_back(block);

    //     blocks.swap(tmp_blocks);
    // }

    // if (blocks.size() == 1)
    //     return blocks[0];

    Block out = blocks[0].cloneEmpty();

    { /// Concatenate blocks
        MutableColumns columns = out.mutateColumns();

        for (size_t i = 0; i < columns.size(); ++i)
        {
            columns[i]->reserve(num_rows);
            for (const auto & block : blocks)
            {
                const auto & tmp_column = *block.getByPosition(i).column;
                columns[i]->insertRangeFrom(tmp_column, 0, block.rows());
            }
        }

        out.setColumns(std::move(columns));
    }

    return out;
}

void HashJoin::joinBlock(Block & block, ExtraBlockPtr & not_processed)
{
    Blocks blocks;
    Block block_orig = block;

    size_t total_rows = 0;


    for (auto & impl : implv)
    {
        block = block_orig;

        auto rows_before = block.rows();
        auto columns_before = block.columns();

        LOG_DEBUG(log, "before HashJoinImpl::joinBlock");

        impl->joinBlock(block, not_processed);
        LOG_DEBUG(log, "impl joinBlock ========================= before: rows {} columns {}, after: rows {} columns {}", rows_before, columns_before, block.rows(), block.columns());

        blocks.push_back(block);

        total_rows += block.rows();

    }

    if (total_rows && !blocks.empty())
    {
        block = mergeBlocks(std::move(blocks), total_rows);
    }

    LOG_DEBUG(log, "impl joinBlock result =========================  rows {} columns {}", block.rows(), block.columns());

}


bool HashJoin::addJoinedBlock(const Block & block, bool check_limits)
{
    bool ret = false;


    for (auto & impl : implv)
    {

        LOG_DEBUG(log, "calling impl addJoinedBlock ========================= rows {} columns {}", block.rows(), block.columns());
        if (impl->addJoinedBlock(block, check_limits))
        {
            ret = true;
        }
    }

    return ret;
}


#endif

}
