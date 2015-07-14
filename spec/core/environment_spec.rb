require "spec_helper"
require "logstash/environment"

describe LogStash::Environment do

  context "when loading jars dependencies" do

    let(:default_jars_location)    { File.join("vendor", "jar-dependencies") }
    let(:default_runtime_location) { File.join(default_jars_location,"runtime-jars","*.jar") }
    let(:default_test_location)    { File.join(default_jars_location,"test-jars","*.jar") }

    it "raises an exception if jruby is not available" do
      expect(subject).to receive(:jruby?).and_return(false)
      expect { subject.load_runtime_jars! }.to raise_error
    end

    it "find runtime jars in the default location" do
      expect(subject).to receive(:find_jars).with(default_runtime_location).and_return([])
      subject.load_runtime_jars!
    end

    it "find test jars in the default location" do
      expect(subject).to receive(:find_jars).with(default_test_location).and_return([])
      subject.load_test_jars!
    end

    context "when loading a jar file" do

      let(:dummy_jar_file) { File.join(default_jars_location,"runtime-jars","elasticsearch.jar") }

      it "requires the jar files if there are jars to load" do
        expect(subject).to receive(:find_jars).with(default_runtime_location).and_return([dummy_jar_file])
        expect(subject).to receive(:require).with(dummy_jar_file)
        subject.load_runtime_jars!
      end

      it "raises an exception if there are no jars to load" do
        allow(Dir).to receive(:glob).and_return([])
        expect { subject.load_runtime_jars! }.to raise_error
      end
    end
  end

  context "plugin paths" do

    let(:path)      { "/some/path" }
    let(:load_path) { $LOAD_PATH }

    before(:each) do
      expect(load_path).to_not include(path)
    end

    after(:each) do
      load_path.delete(path)
    end

    it "should add the path to the $LOAD_PATH" do
      subject.add_plugin_path(path)
      expect(load_path).to include(path)
    end

    it "should change the LOAD_PATH size by one" do
      expect{subject.add_plugin_path(path)}.to change{load_path.size}.by(1)
    end
  end
end
