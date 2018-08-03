require 'tiny_tds'                              # подгружаем Gem: "Tini_tds" (gem install tiny_tds)
require 'yaml'                                  # подгружаем YAML

Encoding.default_external='utf-8'               # задаем кодировку utf-8 для внешних данных (чтобы решить проблему кириллицы в MSSQL)
Encoding.default_internal='utf-8'               # задаем кодировку utf-8 для внутренних данных

# подгружаем конфиг из config/mssql_config.yml
config = YAML.load_file(File.join(File.dirname(__FILE__),'config/', 'config.yml'))


########################################################################################################################################################################################
# конфигурация подключения к серверу MSSQL
$mssql_dataserver = config['mssql_dataserver']  # сервер на котором база данных # \\ тут не спроста, именно двойной!!!
$mssql_name = config['mssql_name']              # имя базы данных
$mssql_user = config['mssql_user']              # логин
$mssql_pass = config['mssql_pass']              # пароль
$mssql_port = config['mssql_port']              # порт сервера
$mssql_azure = config['mssql_azure']            # Azure? True или False?

# конфигурация работы с таблицой
$mssql_table1 = config['mssql_table1']          # таблица к которой обращаемся
$mssql_limit = config['mssql_limit']            # ограничиваем количество возвращаемых строк
$mssql_what = config['mssql_what']              # какие столбцы запрашиваем
$mssql_where = config['mssql_where']            # фильтруем по условию
$mssql_group = config['mssql_group']            # группируем ли?
$mssql_order = config['mssql_order']            # сортируем ли?

########################################################################################################################################################################################

# подключаемся к базе данных
client = TinyTds::Client.new(username: $mssql_user, password: $mssql_pass, dataserver: $mssql_dataserver, port: $mssql_port, database: $mssql_name, azure: $mssql_azure)

# получаем список таблиц базы данных
tables_list = client.execute("Select Table_name From Information_schema.Tables Where Table_type = 'BASE TABLE' and Objectproperty (Object_id(Table_name), 'IsMsShipped') = 0")
tables_list.each(:symbolize_keys => false) do |row|
  puts row
  end
puts ""

puts "Введите имя таблицы:"
@select_table = gets.chomp
puts ""

# получаем список столбцов таблицы
columns_list = client.execute("SELECT name FROM sys.columns WHERE object_id = OBJECT_ID('dbo.#{@select_table}')")
columns_list.each(:symbolize_keys => false) do |row|
  puts row
  end
puts ""

# вызываем выборку
query = client.execute("SELECT #{$mssql_limit} #{$mssql_what} FROM dbo.#{@select_table} #{$mssql_where} #{$mssql_group} #{$mssql_order}")
query.each(:symbolize_keys => false) do |row|
  puts row
  puts ""
  end
