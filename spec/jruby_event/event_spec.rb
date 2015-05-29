$LOAD_PATH << File.expand_path("../../../lib", __FILE__)

require "jruby_event/jruby_event"

describe LogStash::Event do
  context "to_json" do
    it "should serialize snmple values" do
      e = LogStash::Event.new({"foo" => "bar", "bar" => 1, "baz" => 1.0, "@timestamp" => "2015-05-28T23:02:05.350Z"})
      expect(e.to_json).to eq("{\"foo\":\"bar\",\"bar\":1,\"baz\":1.0,\"@timestamp\":\"2015-05-28T23:02:05.350Z\",\"@version\":\"1\"}")
    end

    it "should serialize deep hash values" do
      e = LogStash::Event.new({"foo" => {"bar" => 1, "baz" => 1.0, "biz" => "boz"}, "@timestamp" => "2015-05-28T23:02:05.350Z"})
      expect(e.to_json).to eq("{\"foo\":{\"bar\":1,\"baz\":1.0,\"biz\":\"boz\"},\"@timestamp\":\"2015-05-28T23:02:05.350Z\",\"@version\":\"1\"}")
    end

    it "should serialize deep array values" do
      e = LogStash::Event.new({"foo" => ["bar", 1, 1.0], "@timestamp" => "2015-05-28T23:02:05.350Z"})
      expect(e.to_json).to eq("{\"foo\":[\"bar\",1,1.0],\"@timestamp\":\"2015-05-28T23:02:05.350Z\",\"@version\":\"1\"}")
    end

    it "should serialize deep hash from field reference assignments" do
      e = LogStash::Event.new({"@timestamp" => "2015-05-28T23:02:05.350Z"})
      e["foo"] = "bar"
      e["bar"] = 1
      e["baz"] = 1.0
      e["[fancy][pants][socks]"] = "shoes"
      expect(e.to_json).to eq("{\"@timestamp\":\"2015-05-28T23:02:05.350Z\",\"@version\":\"1\",\"foo\":\"bar\",\"bar\":1,\"baz\":1.0,\"fancy\":{\"pants\":{\"socks\":\"shoes\"}}}")
    end
  end

  context "[]" do
    it "should get simple values" do
      e = LogStash::Event.new({"foo" => "bar", "bar" => 1, "baz" => 1.0, "@timestamp" => "2015-05-28T23:02:05.350Z"})
      expect(e["foo"]).to eq("bar")
      expect(e["[foo]"]).to eq("bar")
      expect(e["bar"]).to eq(1)
      expect(e["[bar]"]).to eq(1)
      expect(e["baz"]).to eq(1.0)
      expect(e["[baz]"]).to eq(1.0)
      expect(e["@timestamp"].to_s).to eq("2015-05-28T23:02:05.350Z")
      expect(e["[@timestamp]"].to_s).to eq("2015-05-28T23:02:05.350Z")
    end

    it "should get deep hash values" do
      e = LogStash::Event.new({"foo" => {"bar" => 1, "baz" => 1.0}})
      expect(e["[foo][bar]"]).to eq(1)
      expect(e["[foo][baz]"]).to eq(1.0)
    end

    it "should get deep array values" do
      e = LogStash::Event.new({"foo" => ["bar", 1, 1.0]})
      expect(e["[foo][0]"]).to eq("bar")
      expect(e["[foo][1]"]).to eq(1)
      expect(e["[foo][2]"]).to eq(1.0)
      expect(e["[foo][3]"]).to be_nil
    end
  end

  context "[]=" do
    it "should set simple values" do
      e = LogStash::Event.new()
      expect(e["foo"] = "bar").to eq("bar")
      expect(e["foo"]).to eq("bar")

      e = LogStash::Event.new({"foo" => "test"})
      expect(e["foo"] = "bar").to eq("bar")
      expect(e["foo"]).to eq("bar")
    end

    it "should set deep hash values" do
      e = LogStash::Event.new()
      expect(e["[foo][bar]"] = "baz").to eq("baz")
      expect(e["[foo][bar]"]).to eq("baz")
      expect(e["[foo][baz]"]).to be_nil
    end

    it "should set deep array values" do
      e = LogStash::Event.new()
      expect(e["[foo][0]"] = "bar").to eq("bar")
      expect(e["[foo][0]"]).to eq("bar")
      expect(e["[foo][1]"] = 1).to eq(1)
      expect(e["[foo][1]"]).to eq(1)
      expect(e["[foo][2]"] = 1.0 ).to eq(1.0)
      expect(e["[foo][2]"]).to eq(1.0)
      expect(e["[foo][3]"]).to be_nil
    end
  end
end
``