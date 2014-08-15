require 'rbbt-util'
require 'rbbt/workflow'

require 'rbbt/sources/EVS'

Workflow.require_workflow "Genomes1000"

module EVS
  extend Workflow

  dep Genomes1000, :identify
  task :annotate => :tsv do 
    database = EVS.database
    dumper = TSV::Dumper.new :key_field => "Genomic Mutation", :fields => database.fields, :type => :list, :cast => :to_f
    dumper.init
    TSV.traverse step(:identify), :into => dumper, :bar => "EVS" do |mutation,rsid|
      mutation = mutation.first if Array === mutation
      values = database[rsid] 
      next if values.nil?
      [mutation, values]
    end
  end
end
