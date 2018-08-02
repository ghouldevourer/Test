#пробуем соединяться с БД MySQL

require "Mysql2"    # подключаем модуль MySQL

#исходные данные
@db_host  = "db4free.net" #указываем хост
@db_user  = "rubytest" #указываем логин
@db_pass  = "rubytest" #указываем пароль
@db_name = "rubytest" #указываем имя базы

#sql7250225@ec2-52-8-112-233.us-west-1.compute.amazonaws.com
#ec2-52-8-112-233.us-west-1.compute.amazonaws.com


#подключение
client = Mysql2::Client.new(:host => @db_host, :username => @db_user, :password => @db_pass, :database => @db_name)

#SQL запрос
@cdr_result = client.query("SELECT * from Test_table")
puts "#{@cdr_result}"
