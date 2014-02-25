require 'yaml'
require 'optparse'

require './lib/snapshot.rb'

current_dir = Dir::pwd

setting_yaml = current_dir.to_s + '/setting.yml'
state_yaml = current_dir.to_s + '/state.yml'

params = {}
OptionParser.new do |parser|
  parser.on('--dry-run') {|v| params[:dry_run] = v}
  parser.parse!(ARGV)
end
dry_run = params[:dry_run]

snapshot = Snapshot.new
snapshot.run(setting_yaml, state_yaml, current_dir, dry_run)

