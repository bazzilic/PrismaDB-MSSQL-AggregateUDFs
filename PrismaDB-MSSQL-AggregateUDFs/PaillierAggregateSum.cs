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
        private BigInteger accumulatorAct;
        private BigInteger accumulatorNeg;
        private BigInteger cachedNSq;
        private bool isEmpty;

        public void Init()
        {
            isEmpty = true;
        }

        public void Accumulate(SqlBytes p_toAdd, SqlBytes p_NSquare)
        {
            var bytes = p_toAdd.Buffer;
            var bytesActual = new byte[bytes.Length / 2];
            Array.Copy(bytes, bytesActual, bytes.Length / 2);
            var bytesNegative = new byte[bytes.Length / 2];
            Array.Copy(bytes, bytes.Length / 2, bytesNegative, 0, bytes.Length / 2);

            var biActual_toAdd = new BigInteger(bytesActual);
            var biNegative_toAdd = new BigInteger(bytesNegative);

            if (isEmpty)
            {
                accumulatorAct = biActual_toAdd;
                accumulatorNeg = biNegative_toAdd;
                cachedNSq = new BigInteger(p_NSquare.Buffer);
                isEmpty = false;
                return;
            }

            accumulatorAct = (accumulatorAct * biActual_toAdd) % cachedNSq;
            accumulatorNeg = (accumulatorNeg * biNegative_toAdd) % cachedNSq;
        }

        public void Merge(PaillierAggregateSum anotherInstance)
        {
            if (anotherInstance.isEmpty)
                return;

            if (isEmpty)
            {
                accumulatorAct = anotherInstance.accumulatorAct;
                accumulatorNeg = anotherInstance.accumulatorNeg;
                cachedNSq = anotherInstance.cachedNSq;
                isEmpty = false;
            }
            else
            {
                accumulatorAct = (accumulatorAct * anotherInstance.accumulatorAct) % cachedNSq;
                accumulatorNeg = (accumulatorNeg * anotherInstance.accumulatorNeg) % cachedNSq;
            }
        }

        public SqlBytes Terminate()
        {
            var resActual = accumulatorAct.ToByteArray();
            var resNegative = accumulatorNeg.ToByteArray();

            var maxLength = resActual.Length > resNegative.Length ? resActual.Length : resNegative.Length;

            var res = new byte[maxLength * 2];
            Array.Copy(resActual, 0, res, 0, resActual.Length);
            Array.Copy(resNegative, 0, res, res.Length / 2, resNegative.Length);

            return new SqlBytes(res);
        }

        public void Read(BinaryReader r)
        {
            isEmpty = r.ReadBoolean();
            var accActualLength = r.ReadInt32();
            var accActualBytes = r.ReadBytes(accActualLength);
            var accNegativeLength = r.ReadInt32();
            var accNegativeBytes = r.ReadBytes(accNegativeLength);
            var NsqLength = r.ReadInt32();
            var NsqBytes = r.ReadBytes(NsqLength);

            accumulatorAct = new BigInteger(accActualBytes);
            accumulatorNeg = new BigInteger(accNegativeBytes);
            cachedNSq = new BigInteger(NsqBytes);
        }

        public void Write(BinaryWriter w)
        {
            var accActualBytes = accumulatorAct.ToByteArray();
            Int32 accActualLength = accActualBytes.Length;
            var accNegativeBytes = accumulatorNeg.ToByteArray();
            Int32 accNegativeLength = accNegativeBytes.Length;
            var NsqBytes = cachedNSq.ToByteArray();
            Int32 NsqLength = NsqBytes.Length;

            w.Write(isEmpty);
            w.Write(accActualLength);
            w.Write(accActualBytes);
            w.Write(accNegativeLength);
            w.Write(accNegativeBytes);
            w.Write(NsqLength);
            w.Write(NsqBytes);
        }
    }
}
