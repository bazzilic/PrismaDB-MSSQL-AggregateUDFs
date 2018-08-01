using Microsoft.SqlServer.Server;
using System;
using System.Data.SqlTypes;
using System.IO;
using System.Numerics;

namespace PrismaDB.MSSQL.AggregateUDFs
{
    [Serializable]
    [SqlUserDefinedAggregate(
            Format.UserDefined,     // https://docs.microsoft.com/en-us/dotnet/api/microsoft.sqlserver.server.format?view=netframework-4.5
            IsInvariantToNulls = true,
            IsInvariantToDuplicates = false,
            IsInvariantToOrder = true,
            MaxByteSize = 8000)]
    public class PaillierAggregateSum : IBinarySerialize
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
                cachedNSq = anotherInstance.cachedNSq;
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

        public void Read(BinaryReader r)
        {
            isEmpty = r.ReadBoolean();
            var accLength = r.ReadInt32();
            var accBytes = r.ReadBytes(accLength);
            var NsqLength = r.ReadInt32();
            var NsqBytes = r.ReadBytes(NsqLength);

            accumulator = new BigInteger(accBytes);
            cachedNSq = new BigInteger(NsqBytes);
        }

        public void Write(BinaryWriter w)
        {
            var accBytes = accumulator.ToByteArray();
            Int32 accLength = accBytes.Length;
            var NsqBytes = cachedNSq.ToByteArray();
            Int32 NsqLength = NsqBytes.Length;

            w.Write(isEmpty);
            w.Write(accLength);
            w.Write(accBytes);
            w.Write(NsqLength);
            w.Write(NsqBytes);
        }
    }
}
