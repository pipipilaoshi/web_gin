package main

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"os"
	"strconv"
)

//sql包提供了保证SQL或类SQL数据库的泛用接口。使用sql包时必须注入（至少）一个数据库驱动。
// 参见http://golang.org/s/sqldrivers 获取驱动列表。如使用mysql，则需要注入github.com/go-sql-driver/mysql。
import (
	"fmt"
)

//定义一个接收数据库表字段的结构体
type DbPerson struct {
	Id   int    `db:"id"`
	Name string `db:"name"`
	Age  int    `db:"age"`
	Sex  int    `db:"gender"`
}

//定义一个通用的错误处理函数
func ErrorHandler(err error, where string) {
	if err != nil {
		fmt.Println("出现错误：", err, where)
		os.Exit(1)
	}
}

func main() {
	//1.数据库连接
	port := "3306"
	host := "localhost"
	userName := "root"
	passWard := "123456"
	//"username:password@tcp(localhost:3306)/go_mysql_demo?charset=utf8"
	dataSourceName := userName + ":" + passWard + "@tcp(" + host + ":" + port + ")/users"
	db, err := sql.Open("mysql", dataSourceName)
	ErrorHandler(err, "sql.Open()")
	defer func() {
		err := db.Close()
		ErrorHandler(err, "db.Close()")
	}()
	sqlMakeDBTable(db)
	sqlInsert(db)
	os.Exit(8)
	//1.增操作
	var insertSql = "insert into users (name,age,gender) value (?,?,?) (?,?,?) (?,?,?)"
	stmt, err := db.Prepare(insertSql)
	//准备sql语句，预执行语句，返回*Stmt声明句柄，stmt的主要方法:Exec、Query、QueryRow、Close
	ErrorHandler(err, "db.Prepare()")
	//执行
	res, err := stmt.Exec("fun", 11, 1, "john", 12, 1, "amy", 14, 0)
	ErrorHandler(err, "stmt.Exec()")
	//获取最后一个插入执行结果
	lastInsertId, err := res.LastInsertId()
	ErrorHandler(err, "res.LastInsertId()")

	//log.Println(reflect.TypeOf(lastInsertId))    //打印变量类型
	//将int64转换为字符串
	lastid := strconv.FormatInt(lastInsertId, 10)
	log.Println("lastInsertId = " + lastid)

	//2.改操作
	updateSql := `update users set age=? where name=?`
	stmt, err = db.Prepare(updateSql)
	ErrorHandler(err, "db.Prepare()")
	//执行
	res, err = stmt.Exec(18, "fun")
	ErrorHandler(err, "stmt.Exec()")
	affectCount, _ := res.RowsAffected()
	log.Printf("%v", affectCount)

	//3.删操作
	deleteSql := "delete from users where name=?"
	stmt, err = db.Prepare(deleteSql)
	ErrorHandler(err, "db.Prepare()")
	res, err = stmt.Exec("amy")
	ErrorHandler(err, "stmt.Exec()")

	//4.查询操作
	//查询一条记录，必须使用一个接收变量
	var user *DbPerson
	findSql := "select * from users where id = ?"
	//查询一条，返回一条结果。并赋值到user这个结构体类型的变量中,就算查询到的是多条，单返回的还是一条
	err = db.QueryRow(findSql, 11).Scan(&user.Id, &user.Name, &user.Sex)
	ErrorHandler(err, "db.QueryRow")
	log.Println(user)

	//查询多条记录
	selectSql := "select * from users"
	selectRows, err := db.Query(selectSql)
	ErrorHandler(err, "db.Query")
	defer func() {
		err = selectRows.Close()
		ErrorHandler(err, "selectRows.Close")
	}()

	for selectRows.Next() {
		var person DbPerson
		if err := selectRows.Scan(&person); err != nil {
			ErrorHandler(err, "selectRows.Scan")
			return
		}
		log.Printf("person:%v", person)
	}

	//5.事务操作
	//声明一个事务的开始
	tx, err := db.Begin()
	ErrorHandler(err, "db.Begin")

	//开始事务操作
	_, err1 := tx.Exec("insert into person(name,age,gender) values (?,?,?)", "fun5", 30, 1)
	_, err2 := tx.Exec("update person1 set age=? where name=?", 31, "fun1")
	_, err3 := tx.Exec("insert into person(name,age,gender) values (?,?,?)", "fun6", 30, 2)

	if err1 != nil || err2 != nil || err3 != nil {
		fmt.Println("事务操作出错，开始回滚")
		tx.Rollback()
	} else {
		fmt.Println("事务操作成功！")
		tx.Commit()
	}

}

func sqlInsert(db *sql.DB) {
	//1.增操作
	var insertSql = "insert into users  value (\"fun\", 11, 1), (\"john\", 12, 1), (\"amy\", 14, 0)"
	res, err := db.Exec(insertSql)
	//准备sql语句，预执行语句，返回*Stmt声明句柄，stmt的主要方法:Exec、Query、QueryRow、Close
	ErrorHandler(err, "db.Prepare()2")
	//执行
	//res, err := stmt.Exec("fun", 11, 1, "john", 12, 1, "amy", 14, 0)
	ErrorHandler(err, "stmt.Exec()3")
	//获取最后一个插入执行结果
	lastInsertId, err := res.LastInsertId()
	ErrorHandler(err, "res.LastInsertId()")

	//log.Println(reflect.TypeOf(lastInsertId))    //打印变量类型
	//将int64转换为字符串
	lastid := strconv.FormatInt(lastInsertId, 10)
	log.Println("lastInsertId = " + lastid)
}

func sqlMakeDBTable(db *sql.DB) {
	//1.增操作
	var insertSql = "DROP TABLE IF EXISTS `users`;"
	res, err := db.Exec(insertSql)
	insertSql =
		"CREATE TABLE `users`" +
			" (`name`  varchar(50) COLLATE utf8_bin NOT NULL DEFAULT '' COMMENT '角色名'," +
			"`gender` int(10) NOT NULL DEFAULT '0' COMMENT '心魔等级'," +
			"`age` int(10) NOT NULL DEFAULT '0' COMMENT '击杀时间'," +
			//"  PRIMARY KEY (`id`) USING BTREE," +
			" PRIMARY KEY `name` (`name`) USING BTREE," +
			"  KEY `gender` (`gender`) USING BTREE," +
			"  KEY `age` (`age`) USING BTREE) "
	res, err = db.Exec(insertSql)
	//准备sql语句，预执行语句，返回*Stmt声明句柄，stmt的主要方法:Exec、Query、QueryRow、Close
	ErrorHandler(err, "db.Prepare()")
	//执行
	//res, err := stmt.Exec("fun", 11, 1, "john", 12, 1, "amy", 14, 0)
	//ErrorHandler(err, "stmt.Exec()")
	//获取最后一个插入执行结果
	lastInsertId, err := res.LastInsertId()
	ErrorHandler(err, "res.LastInsertId()")

	//log.Println(reflect.TypeOf(lastInsertId))    //打印变量类型
	//将int64转换为字符串
	lastid := strconv.FormatInt(lastInsertId, 10)
	log.Println("lastInsertId = " + lastid)
}
