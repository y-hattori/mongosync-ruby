require 'mongo'
require 'logger'
require 'pry'

include Mongo

module Mongosync
  def usage
    "Usage:\n#{$0} src_host:src_port/src_db/src_collection dst_host:dst_port/dst_db/dst_collection"
  end

  # 引数のperse
  def parse_mongo_url(url)
    url =~ /^(.+)\:(\d+)\/(.+)\/(.+)$/
    return $1,$2,$3,$4
  end

  def sync(src_mongo_url, dst_mongo_url)
    src_host,src_port,src_db,src_collection = parse_mongo_url(src_mongo_url)
    dst_host,dst_port,dst_db,dst_collection = parse_mongo_url(dst_mongo_url)
    
    # connecting to the database
    client_src = MongoClient.new(src_host, src_port) # defaults to localhost:27017
    db_src = client_src[src_db]
    coll_src = db_src[src_collection]
    
    client_dst = MongoClient.new(dst_host, dst_port) # defaults to localhost:27017
    db_dst = client_dst[dst_db]
    coll_dst = db_dst[dst_collection]
    
    newest_doc = coll_dst.find().sort([:updated_at, :desc]).limit(1).first
    unless newest_doc
      #sync先のcollectionが空の場合は、全件取得するため、sync元の一番古いコレクションのupdated_atより前の時間を指定
      newest_doc = Hash.new
    binding.pry
      newest_doc["updated_at"] = coll_src.find().sort([:updated_at, :asc]).limit(1).first["updated_at"] - 1
    end
    
    a = coll_src.find({:updated_at => {'$gt' => newest_doc["updated_at"]}}).sort([:updated_at, :asc])
    binding.pry
    
    a.each_with_index do |doc,i|
      doc_dst = coll_dst.find_one({"_id" => doc["_id"]})
      if doc_dst
        doc_dst.update(doc)
      else
        coll_dst.insert(doc)
      end
      puts "[#{Time.now.strftime("%y/%m/%d %H:%M:%S")}] #{i}/#{a.count}" if (i % 1000) == 0
    end
  end
end

