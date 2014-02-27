require 'mysql2'
require 'date'
require 'fileutils'
require 'yaml'
require 'logger'

class Snapshot

  def run (setting_yaml, state_yaml, current_dir, dry_run)

    FileUtils.mkdir_p current_dir + "/log"
    logger = Logger.new("./log/#{Date.today()}.log")

    setting = YAML.load_file(setting_yaml)
    mysql_username  = setting['mysql_username']
    mysql_password  = setting['mysql_password']
    data_dir        = setting['data_dir'] || current_dir + '/data'
    row_scale       = setting['default_row_scale']
    interval        = setting['default_interval']
    rules           = setting['rule']

    yesterday = Date.today() - 1
    snapshot_state = !File.exists?(state_yaml) || YAML.load_file(state_yaml).nil? ?
                 {} : YAML.load_file(state_yaml)

    rules.each do |rule|
      # load target table information
      table      = rule['table']
      query_mode = rule['query_mode']
      sleep_time = rule['sleep_time'].nil? ? interval : rule['sleep_time']
      row_scale  = rule['row_scale'].nil? ? row_scale : rule['row_scale']

      FileUtils.mkdir_p [data_dir, table].join('/').to_s unless dry_run
    
      max_id = _check_target_max(rule, mysql_username, mysql_password)
      max_query_count = (max_id / row_scale).to_i
    
      min_id = snapshot_state.nil? || snapshot_state[table].nil? ? 0 : snapshot_state[table].to_i
      min_query_count = (min_id / row_scale).to_i
    
      last_loop = max_query_count - min_query_count
    
      # dump each 
      (min_query_count..max_query_count).each_with_index do |part, index|
    
        output_tsv = [data_dir, table, "#{table}.#{yesterday}.#{rule['host'].gsub(/\./, '-')}.#{part}.tsv"].join('/')

        min = part == min_query_count ? min_id : row_scale * part
        min = min + 1
        max = index == last_loop ? max_id : (row_scale * part) + row_scale
    
        puts "min:#{min} max:#{max} host:#{rule['host']}" 
        next if dry_run
        logger.info "min:#{min} max:#{max} host:#{rule['host']}"

        _collect_target_data(rule, mysql_username, mysql_password, output_tsv, min, max)
    
        puts sleep_time.to_s + 's sleeping...'
        sleep(sleep_time) unless index == last_loop
      end
    
      # update max_id
      _save_state(snapshot_state, state_yaml, table, max_id) if query_mode == 'connection' and not dry_run 

    end

    # gzip compress
    _compress_all(rules, data_dir, yesterday) unless dry_run
  end

  def _check_target_max (rule, mysql_username, mysql_password)
    # check a query target 
    client = Mysql2::Client.new(:host => rule['host'], :username => mysql_username, :password => mysql_password, :database => rule['database'])
    result = client.query("SELECT MAX(id) as MAX_ID FROM #{rule['table']}")
    return result.first['MAX_ID'].to_i
  end

  def _collect_target_data (rule, mysql_username, mysql_password, output_tsv, min, max)
    client = Mysql2::Client.new(:host => rule['host'], :username => mysql_username, :password => mysql_password, :database => rule['database'])
    sql = "SELECT #{rule['columns']} FROM #{rule['table']} WHERE id between #{min} and #{max}"
    results = client.query(sql, :stream => true, :cache_rows => false, :cast => false)
    
    f = File.new(output_tsv, 'a')
    results.each(:as => :array) do |row|
      f.write(row.join("\t") + "\n")
    end
    f.close
  end

  def _save_state (snapshot_state, state_yaml, table, max_id)
    snapshot_state[table] = max_id
    File.open(state_yaml, 'w'){ |f| f.write(snapshot_state.to_yaml)}
  end

  def _compress_all(rules, data_dir, date)
    tables = rules.map{|rule| rule['table']}.uniq
    tables.each do |table|
      Dir.chdir(data_dir)
      Dir.glob("#{table}/#{table}.#{date}*").each do |file|
        full_path = [data_dir, table, file].join('/')
        system("gzip --best --force #{file}")
      end
    end
  end
end
