using Microsoft.SqlServer.Server;
using System;
using System.Data.SqlTypes;
using System.Numerics;

namespace PrismaDB.MSSQL.AggregateUDFs
{
    [Serializable]
    [SqlUserDefinedAggregate(
            Format.Native,       // https://docs.microsoft.com/en-us/dotnet/api/microsoft.sqlserver.server.format?view=netframework-4.5
            IsInvariantToNulls = true,
            IsInvariantToDuplicates = false,
            IsInvariantToOrder = true)]
    public class PaillierAggregateSum
    {
        private BigInteger accumulator;
        private BigInteger cachedNSq;
        private bool isEmpty;

        public void Init()
        {
            isEmpty = true;
        }

        public void Accumulate(SqlBytes p_toAdd, SqlBytes p_NSquare)
        {
            var bi_toAdd = new BigInteger(p_toAdd.Buffer);

            if (isEmpty)
            {
                accumulator = bi_toAdd;
                cachedNSq = new BigInteger(p_NSquare.Buffer);
                isEmpty = false;
                return;
            }
            
            accumulator = (accumulator * bi_toAdd) % cachedNSq;
        }

        public void Merge(PaillierAggregateSum anotherInstance)
        {
            if (anotherInstance.isEmpty)
                return;

            if (isEmpty)
            {
                accumulator = anotherInstance.accumulator;
                isEmpty = false;
            }
            else
            {
                accumulator = (accumulator * anotherInstance.accumulator) % cachedNSq;
            }
        }

        public SqlBytes Terminate()
        {
            return new SqlBytes(accumulator.ToByteArray());
        }
    }
}
