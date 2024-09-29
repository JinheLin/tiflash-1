#include <fmt/format.h>
#include <IO/FileProvider/FileProvider.h>
#include <Storages/DeltaMerge/File/DMFile.h>
#include <Storages/DeltaMerge/ScanContext.h>
#include <TestUtils/TiFlashTestEnv.h>
#include <Storages/DeltaMerge/File/DMFileBlockInputStream.h>

namespace DB::DM
{

class DMFileTools
{
private: 

inline static constexpr ColId column_id = 3;
inline static DB::ContextPtr context;

static DMFilePtr restoreDMFile()
{
    auto key_manager = std::make_shared<MockKeyManager>(false);
    auto file_provider = std::make_shared<FileProvider>(key_manager, false);
    return DMFile::restore(
        file_provider,
        30409,
        30409,
        "/DATA/disk1/jinhelin/dmfile_test",
        DMFileMeta::ReadMode::all(),
        0,
        NullspaceID);
}

template <typename T>
static std::vector<T> readColumn(const DMFilePtr & dmfile, const ColumnDefine & cd)
{
    DMFileBlockInputStreamBuilder builder(*context);

    auto read_all = [](IBlockInputStream & stream) {
        Blocks blocks;
        for (;;)
        {
            auto block = stream.read();
            if (!block)
                break;
            blocks.push_back(std::move(block));
        }
        return vstackBlocks(std::move(blocks));
    };

    auto stream = builder.build(dmfile, 
        {cd},
        {RowKeyRange::newAll(false, 1)},
        std::make_shared<ScanContext>());

    auto block = read_all(*stream);
    const auto & v = *toColumnVectorDataPtr<T>(block.getByPosition(0).column);
    return std::vector<T>{v.begin(), v.end()};
}
public:
static int mainEntry(int /*argc*/, char ** /*argv*/)
{
    DB::tests::TiFlashTestEnv::setupLogger();
    ::DB::tests::TiFlashTestEnv::initializeGlobalContext();
    context = ::DB::tests::TiFlashTestEnv::getContext();

    auto dmfile = restoreDMFile();
    auto type = dmfile->getColumnStat(column_id).type;

    DMFile::buildInvertedIndex(*context, dmfile, column_id);
    fmt::println("Built");

    auto cd = ColumnDefine{column_id, "c_3", type};
    auto v_original = readColumn<UInt64>(dmfile, cd);
    std::sort(v_original.begin(), v_original.end());

    auto cd_inverted = getInvertedIndexColumnDefine(column_id, type);
    auto v_inverted = readColumn<UInt64>(dmfile, cd_inverted);

    for (size_t i = 0; i < v_original.size(); ++i)
    {
        fmt::println("{} {}", v_original[i], v_inverted[i]);
    }
    
    return 0;
}
};

}