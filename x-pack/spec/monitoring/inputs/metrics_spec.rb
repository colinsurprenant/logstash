# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License;
# you may not use this file except in compliance with the Elastic License.

require "logstash-core"
require "logstash/agent"
require "monitoring/inputs/metrics"
require "rspec/wait"
require 'spec_helper'
require "json"
require "json-schema"
require 'monitoring/monitoring'

describe LogStash::Inputs::Metrics do
  let(:xpack_monitoring_interval) { 1 }
  let(:options) { { "collection_interval" => xpack_monitoring_interval,
                      "collection_timeout_interval" => 600 } }
  let(:elasticsearch_url) { nil }
  let(:elasticsearch_username) { nil }
  let(:elasticsearch_password) { nil }


  subject { described_class.new(options) }
  let(:settings) do
    {
        "xpack.monitoring.enabled" => true,
        "xpack.monitoring.elasticsearch.url" => elasticsearch_url,
        "xpack.monitoring.elasticsearch.username" => elasticsearch_username,
        "xpack.monitoring.elasticsearch.password" => elasticsearch_password,
    }
  end

  let(:es_options) do
    {
        'url' => elasticsearch_url,
        'user' => elasticsearch_username,
        'password' => elasticsearch_password
    }
  end

  context "integration" do

    shared_examples_for 'events are added to the queue' do
      it 'should add a stats events to the queue' do
        wait(60).for { stats_events.size }.to eq(1)
      end

      it 'should add two state events to the queue' do
        # Triggered event plus the one from `update`
        wait(60).for { state_events.size }.to eq(2)
      end
    end

    shared_examples_for 'events are not added to the queue' do
      it 'should not add a stats events to the queue' do
        wait(60).for { stats_events.size }.to eq(0)
      end

      it 'should not add a state events to the queue' do
        # Triggered event plus the one from `update`
        wait(60).for { state_events.size }.to eq(0)
      end
    end

    let(:schemas_path) { File.join(File.dirname(__FILE__), "..", "..", "..", "spec", "monitoring", "schemas") }
    let(:queue) { Concurrent::Array.new }

    let(:config) { "input { dummyblockinginput { } } output { null { } }" }

    let(:pipeline_settings) { LogStash::Runner::SYSTEM_SETTINGS.clone.merge({
      "pipeline.id" => "main",
      "config.string" => config,
    }) }

    let(:agent) { LogStash::Agent.new(pipeline_settings) }
    let(:metric) { agent.metric }
    let(:collector) { metric.collector }
    let(:agent_task) { start_agent(agent) }

    # Can't use let because this value can change over time
    def stats_events
      queue.select do |e|
        e.get("[@metadata][document_type]") == "logstash_stats"
      end
    end

    # Can't use let because this value can change over time
    def state_events
      queue.select do |e|
        e.get("[@metadata][document_type]") == "logstash_state"
      end
    end

    context "with pipeline execution" do

      before :each do
        allow(subject).to receive(:fetch_global_stats).and_return({"uuid" => "00001" })
        allow(subject).to receive(:exec_timer_task)
        allow(subject).to receive(:sleep_till_stop)

        agent
        agent_task

        wait(60).for { agent.get_pipeline(:main) }.to_not be_nil

        subject.metric = metric

        subject.register
        subject.run(queue)
        subject.pipeline_started(agent, agent.get_pipeline(:main))
      end

      after :each do
        subject.stop
        agent.shutdown
        agent_task.wait
      end

      context 'after the pipeline is setup' do
        it "should store the agent" do
           expect(subject.agent).to eq(agent)
        end
      end

      describe "#update" do
        before :each do
          # collector.snapshot_metric is timing dependant and if fired too fast will miss some metrics.
          # after some tests a correct metric_store.size is 72 but when it is incomplete it is lower.
          # I guess this 72 is dependant on the metrics we collect and there is probably a better
          # way to make sure no metrics are missing without forcing a hard sleep but this is what is
          # easily observable, feel free to refactor with a better "timing" test here.
          wait(60).for { collector.snapshot_metric.metric_store.size >= 72 }.to be_truthy

          subject.update(collector.snapshot_metric)
        end

        it_behaves_like 'events are added to the queue'

        describe "state event" do
          let(:schema_file) { File.join(schemas_path, "states_document_schema.json") }

          it "should validate against the schema" do
            wait(60).for { state_events.empty? }.to be_falsey
            expect(JSON::Validator.fully_validate(schema_file, state_events.first.to_json)).to be_empty
          end
        end

        describe "#build_event" do
          let(:schema_file) { File.join(schemas_path, "monitoring_document_schema.json") }

          describe "data event" do
            it "has the correct schema" do
              wait(60).for { stats_events.empty? }.to be_falsey
              expect(JSON::Validator.fully_validate(schema_file, stats_events.first.to_json)).to be_empty
            end
          end
        end
      end
    end
  end

  context "unit tests" do
    let(:queue) { double("queue").as_null_object }

    before do
      allow(subject).to receive(:queue).and_return(queue)
    end

    after :each do
      subject.stop
    end

    describe "#update_pipeline_state" do
      let(:pipeline) { double("pipeline") }
      let(:state_event) { double("state event") }

      describe "system pipelines" do
        before(:each) do
          allow(pipeline).to receive(:system?).and_return(true)
          allow(subject).to receive(:emit_event)
          subject.update_pipeline_state(pipeline)
        end

        it "should not emit any events" do
          expect(subject).not_to have_received(:emit_event)
        end
      end

      describe "normal pipelines" do
        before(:each) do
          allow(pipeline).to receive(:system?).and_return(false)
          allow(subject).to receive(:state_event_for).with(pipeline).and_return(state_event)
          allow(subject).to receive(:emit_event)
          subject.update_pipeline_state(pipeline)
        end

        it "should emit an event" do
          expect(subject).to have_received(:emit_event).with(state_event)
        end
      end
    end
  end
end
