require 'yaml'                                  # подгружаем YAML
require 'tiny_tds'                              # подгружаем Gem: "Tini_tds" (gem install tiny_tds) для связи с MSSQL
require 'pg'                                    # подгружаем Gem: "PG (postgresql)" (gem install pg) для связи с PostgreSQL

Encoding.default_external='utf-8'               # задаем кодировку utf-8 для внешних данных (чтобы решить проблему кириллицы)
Encoding.default_internal='utf-8'               # задаем кодировку utf-8 для внутренних данных

# подгружаем конфиг из config/mssql_config.yml
config = YAML.load_file(File.join(File.dirname(__FILE__),'config/', 'config.yml'))

begin_time = Time.now                           # время начала программы
########################################################################################################################################################################################
# конфигурация подключения к серверу MSSQL
$mssql_dataserver = config['mssql_dataserver']  # сервер на котором база данных
$mssql_db = config['mssql_db']                  # имя базы данных
$mssql_user = config['mssql_user']              # логин
$mssql_pass = config['mssql_pass']              # пароль
$mssql_port = config['mssql_port']              # порт сервера
$mssql_azure = config['mssql_azure']            # Azure? True или False?

# конфигурация работы с таблицой
$mssql_table1 = config['mssql_table1']          # таблица к которой обращаемся
  #$mssql_limit = config['mssql_limit']         # ограничиваем количество возвращаемых строк
$mssql_what = config['mssql_what']              # какие столбцы запрашиваем
  #$mssql_where = config['mssql_where']         # фильтруем по условию
  #$mssql_group = config['mssql_group']         # группируем ли?
  #$mssql_order = config['mssql_order']         # сортируем ли?
########################################################################################################################################################################################

########################################################################################################################################################################################
# конфигурация подключения к серверу PostgreSQL
$pg_host = config['pg_host']                    # сервер на котором база данных
$pg_db = config['pg_db']                        # имя базы данных
$pg_user = config['pg_user']                    # логин
$pg_pass = config['pg_pass']                    # пароль
$pg_port = config['pg_port']                    # порт сервера
$pg_options = config['pg_options']              # дополнительные параметры соединения (не использую)
$pg_tty = config['pg_tty']                      # отладочный терминал TTY (не использую)

# конфигурация работы с таблицой
$pg_table1 = config['pg_table1']                # таблица к которой обращаемся
  #$pg_limit = config['pg_limit']               # ограничиваем количество возвращаемых строк
$pg_what = config['pg_what']                    # какие столбцы запрашиваем
  #$pg_where = config['pg_where']               # фильтруем по условию
  #$pg_group = config['pg_group']               # группируем ли?
  #$pg_order = config['pg_order']               # сортируем ли?
########################################################################################################################################################################################

########################################################################################################################################################################################
$for_create = ''                                # переменная для CREATE TABLE
$for_query = ''                                 # переменная для будущего INSERT в postgre
$columns = ''                                # столбцы
########################################################################################################################################################################################

first_time = Time.now                           # время начала заполнения базы

########################################################################################################################################################################################
# вводим имя таблицы
puts "Введите имя таблицы:"
$table = gets.chomp
########################################################################################################################################################################################
$create_array1 = []                             # массив для наименования столбцов
$create_array2 = []                             # массив для типа данных столбцов
$i = 0                                          # счетчик для наполнения массивов create_array
$cols_string = ""                               # текстовая переменная для наполнения запроса на создание таблицы
$z = 1                                          # счетчик цикла для наполнения переменной cols_string

# подключаемся к базе данных MSSQL
client = TinyTds::Client.new(username: $mssql_user, password: $mssql_pass, dataserver: $mssql_dataserver, port: $mssql_port, database: $mssql_db, azure: $mssql_azure)
# выбираем данные из MSSQL и заполняем массивы create_array
  columns_list = client.execute("SELECT name, system_type_id FROM sys.columns WHERE object_id = OBJECT_ID('#{$table}')")
  columns_list.each(:symbolize_keys => false) do |row|
    case row['system_type_id']
    when 56
      $create_array1[$i] = "#{row['name']}"
      $create_array2[$i] = "integer"
      $i += 1
    when 61
      $create_array1[$i] = "#{row['name']}"
      $create_array2[$i] = "timestamp with time zone"
      $i += 1
    when 231
      $create_array1[$i] = "#{row['name']}"
      $create_array2[$i] = "text"
      $i += 1
    end
  end

$columns = "#{$create_array1[0]}"
# наполняем переменную cols_string
  while ($z <  $create_array1.length) do
    $cols_string += "#{$create_array1[$z]} #{$create_array2[$z]}, "
    $columns += ", #{$create_array1[$z]}"
    $z += 1
  end

# наполняем окончательную переменную для запроса на создание
  $for_create = "DROP TABLE IF EXISTS public.#{$table};
  CREATE TABLE public.#{$table}  (
  #{$create_array1[0]} #{$create_array2[0]} NOT NULL DEFAULT 0, #{$cols_string}CONSTRAINT id PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE public.#{$table}
  OWNER TO postgres;"

# выясняем сколько записей в таблице
counting = client.execute("SELECT count(*) FROM #{$table}")
  counting.each(:symbolize_keys => false) do |ms_count|
    print "Количество записей в MSSQL:"
    puts ms_count['']
  end

# подключаемся к postgresql
connect = PG.connect($pg_host, $pg_port, $pg_options, $pg_tty, $pg_db, $pg_user, $pg_pass)

# выполняем запрос на создание таблицы, если таблица присутствует -> удаляем
connect.exec("#{$for_create}") do |result|
end

########################################################################################################################################################################################
# запрашиваем данные
query = client.execute("SELECT #{$mssql_what} FROM #{$table}")
  query.each(:symbolize_keys => false) do |ms_row|

# сохраняем данные в переменной $for_query в формате для SQL запроса
  $for_query =  "#{ms_row['ID']},  '#{ms_row['StartDate']}',  '#{ms_row['StopDate']}',  '#{ms_row['Line']}',  '#{ms_row['Sorg']}',  '#{ms_row['Dest']}',  '#{ms_row['Qta']}',  '#{ms_row['TypeMovement']}',  '#{ms_row['SP']}',  '#{ms_row['Note']}',
  '#{ms_row['SIDC']}',  '#{ms_row['DIDC']}',  '#{ms_row['Status']}',  '#{ms_row['Operatore']}',  '#{ms_row['ServerName']}'"

# записываем данные из $for_query в таблицу wincc_movements
  connect.exec("INSERT INTO #{$table} (#{$columns}) VALUES (#{$for_query})") do |result|
    result.each do |pg_row|
    puts pg_row
  end
end
end

print "Время выполнения переноса:"
puts Time.now - first_time                      # время за которое заполнилась база

########################################################################################################################################################################################
second_time = Time.now                          # время начала удаления дубликатов

# удаляем дубликаты
connect = PG.connect($pg_host, $pg_port, $pg_options, $pg_tty, $pg_db, $pg_user, $pg_pass)
  connect.exec( "DELETE FROM #{$table} WHERE #{$table}.id NOT IN (SELECT id FROM (SELECT DISTINCT ON (#{$columns}) * FROM #{$table}) AS foo)" ) do |result|
    result.each do |pg_row|
    puts pg_row
  end
end

print "Время удаления дубликатов:"
puts Time.now - second_time                     # время удаления дубликатов
########################################################################################################################################################################################
connect = PG.connect($pg_host, $pg_port, $pg_options, $pg_tty, $pg_db, $pg_user, $pg_pass)
counting = connect.exec( "SELECT count(*) FROM #{$table}" ) do |pg_res|
  pg_res.each do |pg_count|
  print "Количество записей в PostgreSQL:"
  puts pg_count['count']
end
end
########################################################################################################################################################################################
print "Итоговое время:"
puts Time.now - begin_time                      # итоговое время
########################################################################################################################################################################################
