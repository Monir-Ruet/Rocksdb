#include<bits/stdc++.h>
using namespace std;
#include "hdfs.h"
#include<rocksdb/db.h>
#define nl '\n'
int main(){

    rocksdb::hdfs a;
    rocksdb::Options option;
    rocksdb::DB * db;
    option.env=&a;
    option.create_if_missing=true;
    rocksdb::Status s=rocksdb::DB::Open(option,"/tmp/db",&db);
    cout<<s.ToString()<<nl;
}