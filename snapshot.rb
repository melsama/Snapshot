#!/usr/bin/env ruby

require 'mysql2'
require 'yaml'
require 'date'
require 'optparse'
require 'fileutils'

current_dir = Dir::pwd

setting_yaml = current_dir.to_s + '/setting.yml'
state_yaml = current_dir.to_s + '/state.yml'

params = {}
OptionParser.new do |parser|
  parser.on('--dry-run') {|v| params[:dry_run] = v}
  parser.parse!(ARGV)
end
dry_run = params[:dry_run]

setting = YAML.load_file(setting_yaml)
mysql_username  = setting['mysql_username']
mysql_password  = setting['mysql_password']
data_dir        = setting['data_dir'] || current_dir + '/data'
row_scale       = setting['row_scale']
interval        = setting['default_interval']
rules           = setting['rule']

yesterday = Date.today() - 1

snapshot_state = !File.exists?(state_yaml) || YAML.load_file(state_yaml).nil? ?
                 {} : YAML.load_file(state_yaml)

rules.each do |rule|
  # load target table information
  database   = rule['database']
  table      = rule['table']
  host       = rule['host']
  query_mode = rule['query_mode']
  columns    = rule['columns']
  sleep_time = rule['sleep_time'].nil? ? interval : rule['sleep_time']

  filename = "#{table}.#{yesterday}.tsv"
  FileUtils.mkdir_p [data_dir, table].join('/').to_s unless dry_run
  output_tsv = [data_dir, table, filename].join('/')

  # check a query target 
  client = Mysql2::Client.new(:host => host, :username => mysql_username, :password => mysql_password, :database => database)
  result = client.query("SELECT MAX(id) as MAX_ID FROM #{table}")

  max_id = result.first['MAX_ID'].to_i
  max_query_count = (max_id / row_scale).to_i

  min_id = snapshot_state.nil? || snapshot_state[table].nil? ? 0 : snapshot_state[table].to_i
  min_query_count = (min_id / row_scale).to_i

  last_loop = max_query_count - min_query_count

  # dump each 
  (min_query_count..max_query_count).each_with_index do |part, index|

    min = part == min_query_count ? min_id : row_scale * part
    min = min + 1
    max = index == last_loop ? max_id : (row_scale * part) + row_scale
    sql = "SELECT #{columns} FROM #{table} WHERE id between #{min} and #{max}"

    puts sql + " #host #{host}"
    next if dry_run

    client = Mysql2::Client.new(:host => host, :username => mysql_username, :password => mysql_password, :database => database)
    results = client.query(sql, :stream => true, :cache_rows => false, :cast => false)

  	f = File.new(output_tsv, 'a')
    results.each(:as => :array) do |row|
      f.write(row.join("\t") + "\n")
    end
    f.close

    puts sleep_time.to_s + 's sleeping...'
    sleep(sleep_time) unless index == last_loop
  end

  # update max_id
  if query_mode == 'connection' and not dry_run then
    snapshot_state[table] = max_id
    File.open(state_yaml, 'w'){ |f| f.write(snapshot_state.to_yaml)}
  end

  # gzip compress
	unless dry_run then
  	puts output_tsv.to_s + ' compress...'
  	system("gzip --best --force #{output_tsv}") unless dry_run
	end
end

