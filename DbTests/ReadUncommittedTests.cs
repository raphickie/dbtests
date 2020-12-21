using System.Data;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using NUnit.Framework;

namespace DbTests
{
	[TestFixture]
	public class ReadUncommittedTests
	{
		private MySqlConnection CreateConnection() => new MySqlConnection(ConnectionStrings.Database);

		[Test]
		public async Task WhenDataChangedInTransaction_ShouldSeeInAnotherTransaction()
		{
			await using var connection1 = CreateConnection();
			await connection1.OpenAsync();
			await using var transaction1 = connection1.BeginTransaction(IsolationLevel.ReadUncommitted);
			await using var myCommand = connection1.CreateCommand();
			myCommand.Connection = connection1;
			myCommand.Transaction = transaction1;
			myCommand.CommandText = "select count(*) from dbtests.testtable where SomeType = 19";
			var rowBeforeCount = (long) await myCommand.ExecuteScalarAsync();


			await using var connection2 = CreateConnection();
			await connection2.OpenAsync();
			var myCommand2 = connection2.CreateCommand();
			myCommand2.Transaction = connection2.BeginTransaction(IsolationLevel.ReadUncommitted);
			myCommand2.CommandText = $"insert into dbtests.testtable(SomeType, SomeString) values ({19}, 'test')";
			await myCommand2.ExecuteNonQueryAsync();


			await using var afterCommand = connection1.CreateCommand();
			afterCommand.Transaction = transaction1;
			afterCommand.CommandText = myCommand.CommandText;
			var rowAfterCount = (long) await myCommand.ExecuteScalarAsync();

			#region Assert

			Assert.AreEqual(rowBeforeCount + 1, rowAfterCount, "ReadUncommitted should be able to see uncommitted data");

			#endregion
		}
	}
}