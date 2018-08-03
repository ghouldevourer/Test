require 'pg'                                    # подгружаем Gem: "PG (postgresql)" (gem install pg)
require 'yaml'                                  # подгружаем YAML

Encoding.default_external='utf-8'               # задаем кодировку utf-8 для внешних данных (чтобы решить проблему кириллицы)
Encoding.default_internal='utf-8'               # задаем кодировку utf-8 для внутренних данных

# подгружаем конфиг из config/config.yml
config = YAML.load_file(File.join(File.dirname(__FILE__),'config/', 'config.yml'))

########################################################################################################################################################################################
# конфигурация подключения к серверу PostgreSQL
$pg_host = config['pg_host']              # сервер на котором база данных
$pg_db = config['pg_db']                  # имя базы данных
$pg_user = config['pg_user']              # логин
$pg_pass = config['pg_pass']              # пароль
$pg_port = config['pg_port']              # порт сервера
$pg_options = config['pg_options']        # дополнительные параметры соединения (не использую)
$pg_tty = config['pg_tty']                # отладочный терминал TTY (не использую)

# конфигурация работы с таблицой
$pg_table1 = config['pg_table1']          # таблица к которой обращаемся
$pg_limit = config['pg_limit']            # ограничиваем количество возвращаемых строк
$pg_what = config['pg_what']              # какие столбцы запрашиваем
$pg_where = config['pg_where']            # фильтруем по условию
$pg_group = config['pg_group']            # группируем ли?
$pg_order = config['pg_order']            # сортируем ли?

########################################################################################################################################################################################

# считываем записи
=begin
connect = PG.connect($pg_host, $pg_port, $pg_options, $pg_tty, $pg_db, $pg_user, $pg_pass)
connect.exec( "SELECT #{$pg_what} FROM #{$pg_table1}" ) do |result|
  result.each do |row|
    puts row
  end
end
=end

# добавляем запись или обновляем
connect = PG.connect($pg_host, $pg_port, $pg_options, $pg_tty, $pg_db, $pg_user, $pg_pass)
connect.exec( "INSERT INTO #{$pg_table1} (startdate, starttime, line) VALUES ('2017-09-25', '10:17:10 +0300', 'TEST') ON CONFLICT (id) DO UPDATE SET startdate = EXCLUDED.startdate" ) do |result|
  result.each do |row|
    puts row
  end
end

# удаляем дубликаты
connect = PG.connect($pg_host, $pg_port, $pg_options, $pg_tty, $pg_db, $pg_user, $pg_pass)
connect.exec( "DELETE FROM #{$pg_table1} WHERE #{$pg_table1}.id NOT IN (SELECT id FROM (SELECT DISTINCT ON (startdate, starttime) * FROM #{$pg_table1}) AS foo)" ) do |result|
  result.each do |row|
    puts row
  end
end
