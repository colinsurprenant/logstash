# encoding: utf-8

require "logstash/plugin"
require "logstash/environment"

describe "inputs/log4j" do

  subject {LogStash::Plugin.lookup("input", "log4j").new("mode" => "client")}

  if LogStash::Environment.jruby?
    it "should register in jruby" do
      # register will try to load jars and raise if it cannot find jars or if org.apache.log4j.spi.LoggingEvent class is not present
      expect {subject.register}.to_not raise_error
    end
  else
    it "should not register in mri" do
      # register will try to load jars and raise if it cannot find jars or if org.apache.log4j.spi.LoggingEvent class is not present
      expect {subject.register}.to raise_error(LogStash::EnvironmentError)
    end
  end
end
