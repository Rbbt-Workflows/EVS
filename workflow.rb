require 'rbbt-util'
require 'rbbt/workflow'

require 'rbbt/sources/EVS'

Workflow.require_workflow "Genomes1000"

module EVS
  extend Workflow

  #dep Genomes1000, :identify
  #task :annotate => :tsv do 
  #  database = EVS.database_rsid
  #  dumper = TSV::Dumper.new :key_field => "Genomic Mutation", :fields => database.fields, :type => :list, :cast => :to_f
  #  dumper.init
  #  TSV.traverse step(:identify), :into => dumper, :bar => "EVS" do |mutation,rsid|
  #    mutation = mutation.first if Array === mutation
  #    values = database[rsid] 
  #    next if values.nil?
  #    [mutation, values]
  #  end
  #end

  input :mutations, :array, "Genomic Mutation", nil, :stream => true
  input :by_position, :boolean, "Identify by position", false
  task :annotate => :tsv do |mutations,by_position|
    database = EVS.database
    dumper = TSV::Dumper.new :key_field => "Genomic Mutation", :fields => database.fields, :type => (by_position ? :double : :list), :organism => EVS.organism
    dumper.init
    database.unnamed = true
    TSV.traverse mutations, :type => :array, :into => dumper, :bar => self.progress_bar("Annotate EVS") do |mutation|
      if by_position
        position = mutation.split(":").values_at(0,1) * ":"
        keys = database.prefix(position + ":")
        next if keys.nil?
        values = keys.collect{|key| database[key] }.uniq
        [mutation, Misc.zip_fields(values)]
      else
        values = database[mutation]
        next if values.nil?
        [mutation, values]
      end
    end
  end
end
