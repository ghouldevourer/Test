require 'tiny_tds'                  # подгружаем Gem: "Tini_tds" (gem install tiny_tds)
Encoding.default_external='utf-8'   # задаем кодировку utf-8 для внешних данных (чтобы решить проблему кириллицы в MSSQL)
Encoding.default_internal='utf-8'   # задаем кодировку utf-8 для внутренних данных

# конфигурация подключения к серверу MSSQL
@db_dataserver = "user-pc1\\wincc"  # сервер на котором база данных # \\ тут не спроста, именно двойной!!!
@db_name = "MESDB"                  # имя базы данных
@db_user = "sa"                     # логин
@db_pass = "13245768"               # пароль
@db_port = 1433                     # порт сервера
@db_azure = false                   # Azure? True или False?

# конфигурация работы с таблицой
@db_table1 = "dbo.Movements"        # таблица к которой обращаемся
@db_limit = "TOP 1"                # ограничиваем количество возвращаемых строк
@db_what = "*"                      # какие столбцы запрашиваем
@db_where = ""                      # фильтруем по условию
@db_group = ""                      # группируем ли?
@db_order = ""                      # сортируем ли?

# подключаемся к базе данных
client = TinyTds::Client.new(username: @db_user, password: @db_pass, dataserver: @db_dataserver, port: @db_port, database: @db_name, azure: @db_azure)

# получаем список таблиц базы данных
tables_list = client.execute("Select Table_name From Information_schema.Tables Where Table_type = 'BASE TABLE' and Objectproperty (Object_id(Table_name), 'IsMsShipped') = 0")
tables_list.each(:symbolize_keys => false) do |row|
puts row
end

# получаем список столбцов таблицы
columns_list = client.execute("SELECT name FROM sys.columns WHERE object_id = OBJECT_ID('dbo.Movements')")
columns_list.each(:symbolize_keys => false) do |row|
puts row
end


# вызываем выборку
results = client.execute("SELECT #{@db_limit} #{@db_what} FROM #{@db_table1} #{@db_where} #{@db_group} #{@db_order}")

# выводим каждую строку через puts
results.each(:symbolize_keys => false) do |row|
puts row
end
