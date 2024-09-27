#pragma once

#include <Storages/S3/S3RandomAccessFile.h>

namespace DB::S3
{
class PrefetchFile : public S3RandomAccessFile
{
public:
    PrefetchFile(std::shared_ptr<TiFlashS3Client> client_ptr_, const String & remote_fname_)
        : S3RandomAccessFile(client_ptr_, remote_fname_) {}

    ~PrefetchFile() override = default;

    off_t seek(off_t offset, int whence) override;

    ssize_t read(char * buf, size_t size) override;

private:
    std::vector<char> buffer;
    
};
}