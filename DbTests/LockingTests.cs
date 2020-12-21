using System;
using System.Data;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using NUnit.Framework;

namespace DbTests
{
	[TestFixture]
	public class LockingTests : DbTestBase
	{
		private MySqlConnection CreateConnection() => new MySqlConnection(ConnectionStrings.Database);

		[Test]
		public async Task WhenSelectForUpdate_ShouldNotHangOnUpdateOfVeryDifferentIndex()
		{
			// DO NOT
			await using var connection0 = CreateConnection();
			await connection0.OpenAsync();
			await using var transaction0 = connection0.BeginTransaction(IsolationLevel.RepeatableRead);
			await using var myCommand0 = connection0.CreateCommand();
			myCommand0.Transaction = transaction0;
			myCommand0.CommandText = "delete from dbtests.testtable where SomeType  in (3,4,5)";
			await myCommand0.ExecuteNonQueryAsync();
			await connection0.CloseAsync();

			await using var connection1 = CreateConnection();
			await connection1.OpenAsync();
			await using var transaction1 = connection1.BeginTransaction(IsolationLevel.RepeatableRead);
			await using var myCommand = connection1.CreateCommand();
			myCommand.Transaction = transaction1;
			myCommand.CommandText = "select count(*) from dbtests.testtable where SomeType > 4 FOR UPDATE";
			var rowBeforeCount = (long) await myCommand.ExecuteScalarAsync();

			await InsertNewStringWithIndexValue(3);
			Assert.Pass("Did not hang on adding new string with index < locked");
		}

		[Test]
		public async Task WhenSelectForUpdate_ShouldHangOnUpdateOfSameIndex()
		{
			await using var connection1 = CreateConnection();
			await connection1.OpenAsync();
			await using var transaction1 = connection1.BeginTransaction(IsolationLevel.RepeatableRead);
			await using var myCommand = connection1.CreateCommand();
			myCommand.Transaction = transaction1;
			myCommand.CommandText = "select count(*) from dbtests.testtable where SomeType = 19 FOR UPDATE";
			var rowBeforeCount = (long) await myCommand.ExecuteScalarAsync();

			try
			{
				await InsertNewStringWithIndexValue(19);
			}

			catch (NullReferenceException e)
			{
				Console.WriteLine(e);
				Assert.Pass();
			}
		}

		[Test]
		public async Task WhenSelectForUpdate_ShouldHangOnUpdateOfSlightlyDifferentIndex()
		{
			await using var connection1 = CreateConnection();
			await connection1.OpenAsync();
			await using var transaction1 = connection1.BeginTransaction(IsolationLevel.RepeatableRead);
			await using var myCommand = connection1.CreateCommand();
			myCommand.Transaction = transaction1;
			myCommand.CommandText = "select count(*) from dbtests.testtable where SomeType = 19 FOR UPDATE";
			var rowBeforeCount = (long) await myCommand.ExecuteScalarAsync();

			try
			{
				await InsertNewStringWithIndexValue(18);
			}
			catch (NullReferenceException e)
			{
				Assert.Pass("Hangs on inserting data for slightly different index");
			}
			Assert.Fail("Should have hanged");
		}

		[Test]
		public async Task WhenSelectForUpdate_ShouldNotHangOnUpdateOfDifferentIndex()
		{
			await using var connection1 = CreateConnection();
			await connection1.OpenAsync();
			await using var transaction1 = await connection1.BeginTransactionAsync();
			await using var myCommand = connection1.CreateCommand();
			myCommand.Transaction = transaction1;
			myCommand.CommandText = "select count(*) from dbtests.testtable where SomeType = 19 FOR UPDATE";
			var rowBeforeCount = (long) await myCommand.ExecuteScalarAsync();

			await InsertNewStringWithIndexValue(1);

			Assert.Pass("Can insert data for other different index");
		}

		// TODO: Lock in share mode (select for share)


	}
}