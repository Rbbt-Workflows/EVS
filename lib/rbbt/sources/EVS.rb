require 'rbbt-util'
require 'rbbt/resource'
require 'rbbt/util/tar'

module EVS
  extend Resource
  self.subdir = "share/databases/EVS"

  EVS.claim EVS['.source'], :proc do |directory|
    url = "http://evs.gs.washington.edu/evs_bulk_data/ESP6500SI-V2-SSA137.protein-hgvs-update.snps_indels.txt.tar.gz"
    io = Open.open(url, :nocache => true)
    Misc.untar(io, directory.find)
  end

  EVS.claim EVS.rsids, :proc do |filename|
    dumper = TSV::Dumper.new :key_field => "RS ID", :fields => ["MAF European American", "MAF African American", "MAF all"],
      :type => :list, :cast => :to_f

    dumper.init

    files = EVS['.source'].glob("*.chr*.txt")

    saver = Thread.new {
      Misc.sensiblewrite(filename, dumper.stream)
    }

    TSV.traverse files.sort, :bar => "EVS files" do |file|
      TSV.traverse file, :sep => " ", :bar => File.basename(file), :type => :single, :key_field => "rsID", :fields => ["MAFinPercent(EA/AA/All)"] do |rsid, value|
        next if rsid == "none"
        values = value.split("/")
        dumper.add rsid, values
      end
    end

    dumper.close
    saver.join

    nil
  end

  def self.database
    @@database ||= EVS.rsids.tsv :persist => true
  end
end
