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
        private byte[] accumulator;
        private BigInteger cachedNSq;
        private bool isEmpty;

        public void Init()
        {
            isEmpty = true;
        }

        public void Accumulate(SqlBytes p_toAdd, SqlBytes p_NSquare)
        {
            var bi_toAdd = p_toAdd.Buffer;

            if (isEmpty)
            {
                accumulator = bi_toAdd;
                cachedNSq = new BigInteger(p_NSquare.Buffer);
                isEmpty = false;
                return;
            }

            accumulator = Add(accumulator, bi_toAdd, cachedNSq);
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
                accumulator = Add(accumulator, anotherInstance.accumulator, cachedNSq);
            }
        }

        public SqlBytes Terminate()
        {
            return new SqlBytes(accumulator);
        }

        public void Read(BinaryReader r)
        {
            isEmpty = r.ReadBoolean();
            var accLength = r.ReadInt32();
            var accBytes = r.ReadBytes(accLength);
            var NsqLength = r.ReadInt32();
            var NsqBytes = r.ReadBytes(NsqLength);

            accumulator = accBytes;
            cachedNSq = new BigInteger(NsqBytes);
        }

        public void Write(BinaryWriter w)
        {
            var accBytes = accumulator;
            Int32 accLength = accBytes.Length;
            var NsqBytes = cachedNSq.ToByteArray();
            Int32 NsqLength = NsqBytes.Length;

            w.Write(isEmpty);
            w.Write(accLength);
            w.Write(accBytes);
            w.Write(NsqLength);
            w.Write(NsqBytes);
        }

        private static byte[] Add(byte[] first, byte[] second, BigInteger NSquare)
        {
            var firstActual = new byte[first.Length / 2];
            Array.Copy(first, firstActual, first.Length / 2);
            var firstNegative = new byte[first.Length / 2];
            Array.Copy(first, first.Length / 2, firstNegative, 0, first.Length / 2);
            var secondActual = new byte[second.Length / 2];
            Array.Copy(second, secondActual, second.Length / 2);
            var secondNegative = new byte[second.Length / 2];
            Array.Copy(second, second.Length / 2, secondNegative, 0, second.Length / 2);

            var addActual = AddParts(firstActual, secondActual, NSquare);
            var addNegative = AddParts(firstNegative, secondNegative, NSquare);

            var add = new byte[first.Length];
            Array.Copy(addActual, 0, add, 0, addActual.Length);
            Array.Copy(addNegative, 0, add, add.Length / 2, addNegative.Length);

            return add;
        }

        private static byte[] AddParts(byte[] first, byte[] second, BigInteger NSquare)
        {
            var A = new BigInteger(first);
            var B = new BigInteger(second);

            var resBi = (A * B) % NSquare;
            var res = resBi.ToByteArray();
            return res;
        }
    }
}
