using System;
using System.Data;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using NUnit.Framework;

namespace DbTests
{
	[TestFixture]
	public class RepeatableReadTests : DbTestBase
	{
		private MySqlConnection CreateConnection() => new MySqlConnection(ConnectionStrings.Database);

		[Test]
		public async Task WhenDataChanged_ShouldIgnoreChangesUntilFinished()
		{
			// Get count from first transaction
			await using var connection1 = CreateConnection();
			await connection1.OpenAsync();
			await using var transaction1 = connection1.BeginTransaction(IsolationLevel.RepeatableRead);
			await using var myCommand = connection1.CreateCommand();
			myCommand.Transaction = transaction1;
			myCommand.CommandText = "select count(*) from dbtests.testtable where SomeType = 19";
			var rowBeforeCount = await myCommand.ExecuteScalarAsync();

			await InsertNewStringWithIndexValue(19);

			var myCommand3 = connection1.CreateCommand();
			myCommand3.Transaction = transaction1;
			myCommand3.CommandText = myCommand.CommandText;
			var rowAfterCount = await myCommand3.ExecuteScalarAsync();

			Assert.AreEqual(rowBeforeCount, rowAfterCount);
		}

		[Test]
		public async Task WhenDataChangedAndUpdateDataInSameTransaction_ShouldSeeChangedValues()

		{
			// Read query
			await using var connection1 = CreateConnection();
			await connection1.OpenAsync();
			await using var transaction1 = connection1.BeginTransaction(IsolationLevel.RepeatableRead);
			await using var myCommand = connection1.CreateCommand();
			myCommand.Transaction = transaction1;
			myCommand.CommandText = "select count(*) from dbtests.testtable where SomeType = 19";
			var rowBeforeCount = (long) await myCommand.ExecuteScalarAsync();

			await InsertNewStringWithIndexValue(19);

			// Update some column with same search
			var myUpdateCommand = connection1.CreateCommand();
			myUpdateCommand.Transaction = transaction1;
			myUpdateCommand.CommandText = "update dbtests.testtable set SomeString='changed' where SomeType = 19";
			await myUpdateCommand.ExecuteNonQueryAsync();

			// Read same query after
			var mySelectFinal = connection1.CreateCommand();
			mySelectFinal.Transaction = transaction1;
			mySelectFinal.CommandText = myCommand.CommandText;
			var rowAfterCount = (long) await myCommand.ExecuteScalarAsync();

			await transaction1.CommitAsync();

			Assert.AreEqual(rowBeforeCount + 1, rowAfterCount,
				"Because repeatable read doesn't protect data changed in transaction");
		}


	}
}