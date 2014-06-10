require "logstash/environment"

describe LogStash::Environment do

  describe "load_elasticsearch_jars!" do

    if LogStash::Environment.jruby?
      it "should load elasticsarch jars" do
        expect{LogStash::Environment.load_elasticsearch_jars!}.to_not raise_error
      end

      it "should raise when cannot find elasticsarch jars" do
        stub_const("LogStash::Environment::JAR_DIR", "/some/invalid/path")
        expect{LogStash::Environment.load_elasticsearch_jars!}.to raise_error(LogStash::EnvironmentError)
      end
    else
      it "should raise on MRI upon load_elasticsearch_jars!" do
        expect{LogStash::Environment.load_elasticsearch_jars!}.to raise_error(LogStash::EnvironmentError)
      end
    end
  end
end
