using System;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace Migration
{
	class Program
	{
		private static Random _random;

		static async Task Main(string[] args)
		{
			_random = new Random();
			await InitializeDatabase();
		}

		static async Task InitializeDatabase()
		{
			string connStr = "server=localhost;user=root;port=3306;password=1234";
			await using var conn = new MySqlConnection(connStr);
			await conn.OpenAsync();
			await CreateSchemaAsync(conn);
			await FillTestTableAsync(conn);
		}

		private static async Task FillTestTableAsync(MySqlConnection conn)
		{
			// insert into dbtests.testtable (SomeType, SomeString) values (18, 'test')

			const string dbFillQuery = @"
CREATE TABLE IF NOT EXISTS `dbtests`.`testtable` (
				  `Id` int(11) NOT NULL AUTO_INCREMENT,
				  `SomeType` int(11) DEFAULT NULL,
				  `SomeString` varchar(100) DEFAULT NULL,
				  `SomeDate` datetime DEFAULT NULL,
				  PRIMARY KEY (`Id`),
				  KEY `idx_testtable_Id` (`Id`),
				  KEY `idx_testtable_SomeType` (`SomeType`)
				) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 AVG_ROW_LENGTH=181;
				  ";
			var dbFillCommand = new MySqlCommand(dbFillQuery, conn);
			await dbFillCommand.ExecuteNonQueryAsync();

			try
			{
				for (int i = 0; i < 500; i++)
				{
					var randomDate = DateTime.Now.AddSeconds(-_random.Next(int.MaxValue));
					var query =
						$@"INSERT INTO `dbtests`.`testTable`(SomeType, SomeString, SomeDate) 
values ({_random.Next(30)},'{Guid.NewGuid()}',@somedate)";
					await using var addedRowCommand = new MySqlCommand(query, conn);
					var par = addedRowCommand.Parameters.Add("@somedate", MySqlDbType.DateTime);
					par.Value = randomDate;
					await addedRowCommand.ExecuteNonQueryAsync();
					if (i % 1000 == 0)
					{
						Console.WriteLine($"Passed {i}");
					}
				}
			}
			catch (Exception e)
			{
				Console.WriteLine(e);
				throw;
			}
		}

		private static async Task CreateSchemaAsync(MySqlConnection conn)
		{
			var sql = "Create schema if not exists dbtests";
			var cmd = new MySqlCommand(sql, conn);
			await cmd.ExecuteNonQueryAsync();
		}

		private static async Task CreateTableAsync(MySqlConnection conn)
		{
			var sql = "Create table if not exists testtable";
			var cmd = new MySqlCommand(sql, conn);
			await cmd.ExecuteNonQueryAsync();
		}
	}
}