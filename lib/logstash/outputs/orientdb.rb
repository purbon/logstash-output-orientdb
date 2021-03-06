require "logstash/namespace"
require "logstash/outputs/base"
require "stud/buffer"
require_relative "orientdb-client"

class LogStash::Outputs::OrientDB < LogStash::Outputs::Base

  include Stud::Buffer

  config_name 'orientdb'

  # The URL used to connect to the database.
  config :url, :validate => :string, :default => "http://localhost:8529"

  # The database that is going to be used
  config :database, :validate => :string, :default => "logstash"

  # The username used when authenticating with ArangoDB
  config :username, :validate => :string, :required => true

  # The password used when authenticating with ArangoDB
  config :password, :validate => :password, :required => true

  # The collection used to log the events, by default it will create a collection
  # per day of logs following the pattern logstash-%{+YYYY-MM-dd}
  config :collection, :validate => :string, :default => "logstash-%{+YYYY-MM-dd}"

  # This plugin uses the bulk index api for improved indexing performance.
  # To make efficient bulk api calls, we will buffer a certain number of
  # events before flushing that out to ArangoDB. This setting
  # controls how many events will be buffered before sending a batch
  # of events.
  config :flush_size, :validate => :number, :default => 500

  # The amount of time since last flush before a flush is forced.
  #
  # This setting helps ensure slow event rates don't get stuck in Logstash.
  # For example, if your `flush_size` is 100, and you have received 10 events,
  # and it has been more than `idle_flush_time` seconds since the last flush,
  # Logstash will flush those 10 events automatically.
  #
  # This helps keep both fast and slow log streams moving along in
  # near-real-time.
  config :idle_flush_time, :validate => :number, :default => 1

  def register
    @host   = Socket.gethostname.force_encoding(Encoding::UTF_8)
    config  = {
      "url" => @url,
      "username" => @username,
      "password" => @password
    }
    @client = OrientDB::Client.new(config)
    @client.connect(database)

  end

  def receive(event)
    return unless output?(event)
    @client.new_document(@database, event)
  end

  def stop
    @client.disconnect
  end

  private

  def collection(event)
    @database[event.sprintf(@collection)]
  end

end
