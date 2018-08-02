require 'tiny_tds'                  # подгружаем Gem: "Tini_tds" (gem install tiny_tds)

# конфигурация подключения к серверу MSSQL
@db_dataserver = "user-pc1\\wincc"  # сервер на котором база данных # \\ тут не спроста, именно двойной!!!
@db_name = "MESDB"                  # имя базы данных
@db_user = "sa"                     # логин
@db_pass = "13245768"               # пароль
@db_port = 1433                     # порт сервера
@db_azure = false                   # Azure? True или False?

# конфигурация работы с таблицой
@db_table1 = "dbo.Movements"        # таблица к которой обращаемся
@db_limit = "TOP 10"                # ограничиваем количество возвращаемых строк
@db_what = "*"                      # какие столбцы запрашиваем
@db_where = ""                      # фильтруем по условию
@db_group = ""                      # группируем ли?
@db_order = ""                      # сортируем ли?

# подключаемся к базе данных
client = TinyTds::Client.new(username: @db_user, password: @db_pass, dataserver: @db_dataserver, port: @db_port, database: @db_name, azure: @db_azure)

# вызываем SQL запрос
results = client.execute("SELECT #{@db_limit} #{@db_what} FROM #{@db_table1} #{@db_where} #{@db_group} #{@db_order}")

# выводим каждую строку через puts
results.each(:symbolize_keys => false) do |row|
puts row
end
