#пробуем соединяться с БД MySQL

require "mysql"    # подключаем модуль MySQL

#исходные данные
@db_host  = "localhost" #указываем хост
@db_user  = "root" #указываем логин
@db_pass  = "root" #указываем пароль
@db_name = "your_db_name" #указываем имя базы

#подключение
client = Mysql::Client.new(:host => @db_host, :username => @db_user, :password => @db_pass, :database => @db_name)

#SQL запрос
@cdr_result = client.query("SELECT * from your_db_table_name')
