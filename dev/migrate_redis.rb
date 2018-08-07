require 'yaml'                                  # подгружаем YAML
require 'redis'                              # подгружаем Gem: "Tini_tds" (gem install tiny_tds) для связи с MSSQL
require 'pg'                                    # подгружаем Gem: "PG (postgresql)" (gem install pg) для связи с PostgreSQL

Encoding.default_external='utf-8'               # задаем кодировку utf-8 для внешних данных (чтобы решить проблему кириллицы)
Encoding.default_internal='utf-8'               # задаем кодировку utf-8 для внутренних данных

# подгружаем конфиг из config/mssql_config.yml
config = YAML.load_file(File.join(File.dirname(__FILE__),'config/', 'config.yml'))

begin_time = Time.now                           # время начала программы
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

=begin

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
=end

# считываем записи
connect = PG.connect($pg_host, $pg_port, $pg_options, $pg_tty, $pg_db, $pg_user, $pg_pass)
connect.exec( "SELECT #{$pg_what} FROM #{$table} #{$pg_limit}" ) do |result|
  result.each do |row|
    puts row
  end
end
