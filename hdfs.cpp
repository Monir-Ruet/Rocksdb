#include<bits/stdc++.h>
#include<rocksdb/file_system.h>
#include<rocksdb/options.h>
#include <rocksdb/env.h>
#include "hdfs.h"
#include <fcntl.h>
#include <dirent.h>

const size_t kDefaultPageSize = 4 * 1024;
#define O_RDONLY 00
using namespace std;
#define nl '\n'


namespace rocksdb{
  inline mode_t GetDBFileMode(bool allow_non_owner_access) {
    return allow_non_owner_access ? 0644 : 0600;
  }
  int cloexec_flags(int flags, const EnvOptions* options) {
    #ifdef O_CLOEXEC
    if (options == nullptr || options->set_fd_cloexec) {
      flags |= O_CLOEXEC;
    }
    #else
      (void)options;
    #endif
    return flags;
    }
    // Implemented from /rocksdb/env/fs_posix.cc
    Status hdfs::NewSequentialFile(const std::string& f,std::unique_ptr<SequentialFile>* r,const EnvOptions& options) {
        int fd=-1;
        int flags = cloexec_flags(O_RDONLY, &options);
        FILE* file=nullptr;
        fd = open(f.c_str(), flags, GetDBFileMode(true));
        file=fdopen(fd,"r");
        if(file==nullptr){
          close(fd);
          return Status::IOError("While opening file for sequentially read", f);
        }
        r->reset(new hdfsSequentialFile(f,file,fd,kDefaultPageSize,options));
        return Status::OK();
    }
    Status hdfs::hdfs::NewRandomAccessFile(const std::string& f,std::unique_ptr<RandomAccessFile>* r,const EnvOptions& options){
        return target_.env->NewRandomAccessFile(f,r,options);
    }
    Status hdfs::NewWritableFile(const std::string& f, std::unique_ptr<WritableFile>* r,const EnvOptions& options){
        return target_.env->NewWritableFile(f,r,options);
    }
      Status hdfs::ReopenWritableFile(const std::string& fname,std::unique_ptr<WritableFile>* result,const EnvOptions& options){
        return target_.env->ReopenWritableFile(fname,result,options);
      }
      Status hdfs::ReuseWritableFile(const std::string& fname,const std::string& old_fname,std::unique_ptr<WritableFile>* r,const EnvOptions& options){
        return target_.env->ReuseWritableFile(fname,old_fname,r,options);
      }
      Status hdfs::NewRandomRWFile(const std::string& fname,std::unique_ptr<RandomRWFile>* result,const EnvOptions& options){
        return target_.env->NewRandomRWFile(fname,result,options);
      }
      Status hdfs::NewMemoryMappedFileBuffer(const std::string& fname,std::unique_ptr<MemoryMappedFileBuffer>* result){
        return target_.env->NewMemoryMappedFileBuffer(fname,result);
      }
      Status hdfs::NewDirectory(const std::string& name,std::unique_ptr<Directory>* result) {
        return target_.env->NewDirectory(name,result);
      }
      Status hdfs::FileExists(const std::string& f) {
        return target_.env->FileExists(f);
      }
      Status hdfs::GetChildren(const std::string& dir,std::vector<std::string>* r) {
        return target_.env->GetChildren(dir,r);
      }
      Status hdfs::GetChildrenFileAttributes(const std::string& dir, std::vector<FileAttributes>* result) {
        return target_.env->GetChildrenFileAttributes(dir,result);
      }
      Status hdfs::DeleteFile(const std::string& f){
        return target_.env->DeleteFile(f);
      }
      Status hdfs::Truncate(const std::string& fname, size_t size) {
        return target_.env->Truncate(fname,size);
      }
      Status hdfs::CreateDir(const std::string& d) {
        return target_.env->CreateDir(d);
      }
      Status hdfs::CreateDirIfMissing(const std::string& d) {
        return target_.env->CreateDirIfMissing(d);
      }
      Status hdfs::DeleteDir(const std::string& d) {
        return target_.env->DeleteDir(d);
      }
      Status hdfs::GetFileSize(const std::string& f, uint64_t* s) {
        return target_.env->GetFileSize(f,s);
      }
      Status hdfs::GetFileModificationTime(const std::string& fname,uint64_t* file_mtime) {
        return target_.env->GetFileModificationTime(fname,file_mtime);
      }
      Status hdfs::RenameFile(const std::string& s, const std::string& t) {
        return target_.env->RenameFile(s,t);
      }
      Status hdfs::LinkFile(const std::string& s, const std::string& t){
        return target_.env->LinkFile(s,t);
      }
      Status hdfs::NumFileLinks(const std::string& fname, uint64_t* count) {
        return target_.env->NumFileLinks(fname,count);
      }
      Status hdfs::AreFilesSame(const std::string& first, const std::string& second,bool* res) {
        return target_.env->AreFilesSame(first,second,res);
      }
      Status hdfs::LockFile(const std::string& f, FileLock** l) {
        return target_.env->LockFile(f,l);
      }
      Status hdfs::UnlockFile(FileLock* l){
        return target_.env->UnlockFile(l);
      }
      Status hdfs::IsDirectory(const std::string& path, bool* is_dir){
        return target_.env->IsDirectory(path,is_dir);
      }
      Status hdfs::LoadLibrary(const std::string& lib_name,const std::string& search_path,std::shared_ptr<DynamicLibrary>* result){
        return target_.env->LoadLibrary(lib_name,search_path,result);
      }
      int hdfs::UnSchedule(void* tag, Priority pri) {
        return target_.env->UnSchedule(tag, pri);
      }
      void hdfs::StartThread(void (*f)(void*), void* a) {
        return target_.env->StartThread(f, a);
      }
      void hdfs::WaitForJoin() {
        return target_.env->WaitForJoin();
      }

      int hdfs::ReserveThreads(int threads_to_be_reserved, Priority pri)  {
        return target_.env->ReserveThreads(threads_to_be_reserved, pri);
      }

      int hdfs::ReleaseThreads(int threads_to_be_released, Priority pri){
        return target_.env->ReleaseThreads(threads_to_be_released, pri);
      }

      Status hdfs::GetTestDirectory(std::string* path) {
        return target_.env->GetTestDirectory(path);
      }
      Status hdfs::NewLogger(const std::string& fname,
          std::shared_ptr<Logger>* result) {
        return target_.env->NewLogger(fname, result);
      }
      uint64_t hdfs::NowMicros(){ return target_.env->NowMicros(); }
      uint64_t hdfs::NowNanos() { return target_.env->NowNanos(); }
      uint64_t hdfs::NowCPUNanos() { return target_.env->NowCPUNanos(); }

      void hdfs::SleepForMicroseconds(int micros) {
        target_.env->SleepForMicroseconds(micros);
      }
      Status hdfs::GetHostName(char* name, uint64_t len) {
        return target_.env->GetHostName(name, len);
      }
      Status hdfs::GetCurrentTime(int64_t* unix_time) {
        return target_.env->GetCurrentTime(unix_time);
      }
      Status hdfs::GetAbsolutePath(const std::string& db_path,
          std::string* output_path) {
        return target_.env->GetAbsolutePath(db_path, output_path);
      }
      void hdfs::SetBackgroundThreads(int num, Priority pri) {
        return target_.env->SetBackgroundThreads(num, pri);
      }
      int hdfs::GetBackgroundThreads(Priority pri) {
        return target_.env->GetBackgroundThreads(pri);
      }

      Status hdfs::SetAllowNonOwnerAccess(bool allow_non_owner_access) {
        return target_.env->SetAllowNonOwnerAccess(allow_non_owner_access);
      }

      void hdfs::IncBackgroundThreadsIfNeeded(int num, Priority pri) {
        return target_.env->IncBackgroundThreadsIfNeeded(num, pri);
      }

      void hdfs::LowerThreadPoolIOPriority(Priority pool) {
        target_.env->LowerThreadPoolIOPriority(pool);
      }

      void hdfs::LowerThreadPoolCPUPriority(Priority pool) {
        target_.env->LowerThreadPoolCPUPriority(pool);
      }
      Status hdfs::LowerThreadPoolCPUPriority(Priority pool, CpuPriority pri){
        return target_.env->LowerThreadPoolCPUPriority(pool, pri);
      }
      std::string hdfs::TimeToString(uint64_t time) {
        return target_.env->TimeToString(time);
      }

      Status hdfs::GetThreadList(std::vector<ThreadStatus>* thread_list) {
        return target_.env->GetThreadList(thread_list);
      }

      ThreadStatusUpdater* hdfs::GetThreadStatusUpdater() const {
        return target_.env->GetThreadStatusUpdater();
      }

      uint64_t hdfs::GetThreadID() const { return target_.env->GetThreadID(); }

      std::string hdfs::GenerateUniqueId() {
        return target_.env->GenerateUniqueId();
      }

      EnvOptions hdfs::OptimizeForLogRead(const EnvOptions& env_options) const {
        return target_.env->OptimizeForLogRead(env_options);
      }
      EnvOptions hdfs::OptimizeForManifestRead(
          const EnvOptions& env_options) const {
        return target_.env->OptimizeForManifestRead(env_options);
      }
      EnvOptions hdfs::OptimizeForLogWrite(const EnvOptions& env_options,
          const DBOptions& db_options) const {
        return target_.env->OptimizeForLogWrite(env_options, db_options);
      }
      EnvOptions hdfs::OptimizeForManifestWrite(const EnvOptions& env_options) const {
        return target_.env->OptimizeForManifestWrite(env_options);
      }
      EnvOptions hdfs::OptimizeForCompactionTableWrite(const EnvOptions& env_options,const ImmutableDBOptions& immutable_ops) const {
        return target_.env->OptimizeForCompactionTableWrite(env_options,immutable_ops);
      }
      EnvOptions hdfs::OptimizeForCompactionTableRead(const EnvOptions& env_options,const ImmutableDBOptions& db_options) const {
        return target_.env->OptimizeForCompactionTableRead(env_options, db_options);
      }
      EnvOptions hdfs::OptimizeForBlobFileRead(const EnvOptions& env_options,const ImmutableDBOptions& db_options) const {
        return target_.env->OptimizeForBlobFileRead(env_options, db_options);
      }
      Status hdfs::GetFreeSpace(const std::string& path, uint64_t* diskfree) {
        // cout<<path<<nl;
        return target_.env->GetFreeSpace(path, diskfree);
      }
      void hdfs::SanitizeEnvOptions(EnvOptions* env_opts) const {
        target_.env->SanitizeEnvOptions(env_opts);
      }
      Status hdfs::PrepareOptions(const ConfigOptions& options){
        return Status::NotSupported();
      }
}