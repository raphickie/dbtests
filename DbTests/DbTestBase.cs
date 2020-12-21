using System;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace DbTests
{
	public class DbTestBase
	{
		protected MySqlConnection CreateConnection() => new MySqlConnection(ConnectionStrings.Database);

		protected async Task InsertNewStringWithIndexValue(int value)
		{
			// Insert values matching first transaction
			await using var connection2 = CreateConnection();
			await connection2.OpenAsync();
			await using var tran = await connection2.BeginTransactionAsync(IsolationLevel.RepeatableRead);

			await using var myCommand2 = connection2.CreateCommand();
			myCommand2.Transaction = tran;
			myCommand2.CommandText = $"insert into dbtests.testtable (SomeType, SomeString) values ({value}, 'test')";
			var cts = new CancellationTokenSource();
			cts.CancelAfter(TimeSpan.FromSeconds(2));
			try
			{
				await myCommand2.ExecuteNonQueryAsync(cts.Token);
				await tran.CommitAsync();

			}
			catch (Exception e)
			{
				Console.WriteLine(e);
				throw;
			}
		}
	}
}