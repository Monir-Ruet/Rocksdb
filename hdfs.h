#pragma once
#include<bits/stdc++.h>
#include <rocksdb/env.h>
#include<rocksdb/file_system.h>
using namespace std;
#define nl '\n'
#define ZNS_ALIGMENT 4096

namespace rocksdb{
  class hdfs : public Env {
    public:
      struct Target {
        Env* env;                    // The raw Env
        std::shared_ptr<Env> guard;  // The guarded Env

        explicit Target(Env* t) : env(t) {}
        Target(){}
        explicit Target(std::unique_ptr<Env>&& t) : guard(t.release()) {
          env = guard.get();
        }
        explicit Target(const std::shared_ptr<Env>& t) : guard(t) {
          env = guard.get();
        }
        void Prepare() {
          env=Env::Default();
          // if (guard.get() != nullptr) {
          //   env = guard.get();
          // } else if (env == nullptr) {
          //   env = Env::Default();
          // }
        }
      };
      explicit hdfs(Env* t){}
      hdfs(){
        target_.Prepare();
      }
      explicit hdfs(std::unique_ptr<Env>&& t){}
      explicit hdfs(const std::shared_ptr<Env>& t){}
      ~hdfs() override{}
      Env* target() const { return target_.env; }
      const char* Name() const override { return target_.env->Name(); }
      Status RegisterDbPaths(const std::vector<std::string>& paths) override {
        return target_.env->RegisterDbPaths(paths);
      }
      Status UnregisterDbPaths(const std::vector<std::string>& paths) override {
        return target_.env->UnregisterDbPaths(paths);
      }

      Status NewSequentialFile(const std::string& f,std::unique_ptr<SequentialFile>* r,const EnvOptions& options) override;
      Status NewRandomAccessFile(const std::string& f,std::unique_ptr<RandomAccessFile>* r,const EnvOptions& options) override ;
      Status NewWritableFile(const std::string& f, std::unique_ptr<WritableFile>* r,const EnvOptions& options) override;
      Status ReopenWritableFile(const std::string& fname,std::unique_ptr<WritableFile>* result,const EnvOptions& options) override;
      Status ReuseWritableFile(const std::string& fname,const std::string& old_fname,std::unique_ptr<WritableFile>* r,const EnvOptions& options) override;
      Status NewRandomRWFile(const std::string& fname,std::unique_ptr<RandomRWFile>* result,const EnvOptions& options) override;
      Status NewMemoryMappedFileBuffer(const std::string& fname,std::unique_ptr<MemoryMappedFileBuffer>* result) override;
      Status NewDirectory(const std::string& name,std::unique_ptr<Directory>* result) override;
      Status FileExists(const std::string& f) override;
      Status GetChildren(const std::string& dir,std::vector<std::string>* r) override ;
      Status GetChildrenFileAttributes(const std::string& dir, std::vector<FileAttributes>* result) override ;
      Status DeleteFile(const std::string& f) override ;
      Status Truncate(const std::string& fname, size_t size) override ;
      Status CreateDir(const std::string& d) override ;
      Status CreateDirIfMissing(const std::string& d) override ;
      Status DeleteDir(const std::string& d) override ;
      Status GetFileSize(const std::string& f, uint64_t* s) override ;
      Status GetFileModificationTime(const std::string& fname,uint64_t* file_mtime) override;
      Status RenameFile(const std::string& s, const std::string& t) override ;
      Status LinkFile(const std::string& s, const std::string& t) override ;
      Status NumFileLinks(const std::string& fname, uint64_t* count) override ;
      Status AreFilesSame(const std::string& first, const std::string& second,bool* res) override ;
      Status LockFile(const std::string& f, FileLock** l) override;
      Status UnlockFile(FileLock* l) override;
      Status IsDirectory(const std::string& path, bool* is_dir) override ;
      Status LoadLibrary(const std::string& lib_name,const std::string& search_path,std::shared_ptr<DynamicLibrary>* result) override ;
      void Schedule(void (*f)(void* arg), void* a, Priority pri,void* tag = nullptr, void (*u)(void* arg) = nullptr) override {
        return target_.env->Schedule(f, a, pri, tag, u);
      }
      int UnSchedule(void* tag, Priority pri) override ;
      void StartThread(void (*f)(void*), void* a) override ;
      void WaitForJoin() override ;
      unsigned int GetThreadPoolQueueLen(Priority pri = LOW) const override {
        return target_.env->GetThreadPoolQueueLen(pri);
      }
      int ReserveThreads(int threads_to_be_reserved, Priority pri) override ;
      int ReleaseThreads(int threads_to_be_released, Priority pri) override ;
      Status GetTestDirectory(std::string* path) override ;
      Status NewLogger(const std::string& fname,std::shared_ptr<Logger>* result) override ;
      uint64_t NowMicros() override;
      uint64_t NowNanos() override ;
      uint64_t NowCPUNanos() override ;
      void SleepForMicroseconds(int micros) override ;
      Status GetHostName(char* name, uint64_t len) override ;
      Status GetCurrentTime(int64_t* unix_time) override;
      Status GetAbsolutePath(const std::string& db_path,std::string* output_path) override ;
      void SetBackgroundThreads(int num, Priority pri) override ;
      int GetBackgroundThreads(Priority pri) override ;
      Status SetAllowNonOwnerAccess(bool allow_non_owner_access) override;
      void IncBackgroundThreadsIfNeeded(int num, Priority pri) override ;
      void LowerThreadPoolIOPriority(Priority pool) override ;
      void LowerThreadPoolCPUPriority(Priority pool) override;
      Status LowerThreadPoolCPUPriority(Priority pool, CpuPriority pri) override;
      std::string TimeToString(uint64_t time) override;
      Status GetThreadList(std::vector<ThreadStatus>* thread_list) override ;
      ThreadStatusUpdater* GetThreadStatusUpdater() const override ;
      uint64_t GetThreadID() const override ;
      std::string GenerateUniqueId() override ;
      EnvOptions OptimizeForLogRead(const EnvOptions& env_options) const override ;
      EnvOptions OptimizeForManifestRead(const EnvOptions& env_options) const override ;
      EnvOptions OptimizeForLogWrite(const EnvOptions& env_options,const DBOptions& db_options) const override ;
      EnvOptions OptimizeForManifestWrite(const EnvOptions& env_options) const override ;
      EnvOptions OptimizeForCompactionTableWrite(const EnvOptions& env_options,const ImmutableDBOptions& immutable_ops) const override ;
      EnvOptions OptimizeForCompactionTableRead(const EnvOptions& env_options,const ImmutableDBOptions& db_options) const override ;
      EnvOptions OptimizeForBlobFileRead(const EnvOptions& env_options,const ImmutableDBOptions& db_options) const override ;
      Status GetFreeSpace(const std::string& path, uint64_t* diskfree) override ;
      void SanitizeEnvOptions(EnvOptions* env_opts) const override ;
      Status PrepareOptions(const ConfigOptions& options) override;


    private:
      Target target_;
  };


  class hdfsSequentialFile : public SequentialFile {
    private:
      std::string   filename_;
      bool          use_direct_io_;
      size_t        logical_sector_size_;
      FILE *        file_;
      int           fd_;

    public:
      hdfsSequentialFile(const std::string& fname, FILE* file,int fd,size_t logical_block_size,const EnvOptions& options){
        filename_=fname;
        file_=file;
        fd_=fd;
        logical_sector_size_=logical_block_size;
      }

      virtual ~hdfsSequentialFile() {}

      // Implemented from rocksdb/env/io_posix.cc
      Status Read(size_t n, Slice* result, char* scratch) override{
        size_t r=0;
        do{
          clearerr(file_);
          r=fread_unlocked(scratch,1,n,file_);
        }while(r==0 && ferror(file_));
        // Reads the content of the files.
        cout<<scratch<<nl;
        *result=Slice(scratch,r);
        return Status::OK();
      }

      Status PositionedRead(std::uint64_t offset, size_t n, Slice* result,char* scratch) override{
        cout<<__func__<<nl;
        return Status::OK();
      }

      Status Skip(std::uint64_t n) override{
        cout<<__func__<<nl;
        return Status::OK();
      }
  };
}
