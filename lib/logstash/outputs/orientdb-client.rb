require "faraday"
require "json"

module OrientDB
  class Client

    attr_reader :conn, :config

    def initialize(config)
      @config = config
      @conn ||= Faraday.new(:url => config['url']) do |faraday|
        faraday.request  :url_encoded
        faraday.response :logger
        faraday.adapter  Faraday.default_adapter
      end
      @conn.basic_auth(@config['user'], @config['password'])
    end

    ##
    # connect to a given database
    # return a JSON structure with server information
    ##
    def connect(database)
      response = conn.get "/connect/#{database}"
      JSON.parse(response.body)
    end

    ##
    # disconnect
    ##
    def disconnect
      conn.get "/disconnect"
    end

    ##
    # create a new database
    ##
    def new_database(database, type)
      response = conn.post "/database/#{database}/#{type}"
      JSON.parse(response.body)
    end

    def new_document(database, document)
      conn.post do |req|
        req.url = "/document/#{database}"
        req.headers['Content-Type'] = 'application/json'
        req.body = document
      end
    end

    def batch_create(database, &block)
      conn.post do |req|
        req.url "/batch/#{database}"
        req.headers['Content-Type'] = 'application/json'
        req.body = build_batch_request(block.call, "u")
      end
    end

    ##
    # Get a document
    ##
    def get_document(database, id)
      response = conn.get "/document/#{database}/#{id}"
      JSON.parse(response.body)
    end

    private

    def build_batch_request(content, op="u")
      operations = content.inject([])  do |acc, record|
        acc << { "type" => "u", "record" => record }
      end
      { "transaction" => true, "operations" =>  operations }
    end
  end
end
