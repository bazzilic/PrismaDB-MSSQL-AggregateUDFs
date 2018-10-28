using System;
using System.Data.SqlTypes;
using Xunit;
using PrismaDB.MSSQL.AggregateUDFs;

namespace UDFTests
{
    public class Tests
    {
        private Random rnd;

        public Tests()
        {
            rnd = new Random();
        }

        [Fact]
        public void TestMergeSimple()
        {
            for (var i = 0; i < 50000; i++)
            {
                var udf1 = new PaillierAggregateSum();
                var udf2 = new PaillierAggregateSum();
                var udf3 = new PaillierAggregateSum();

                var Nsq = RandomBytes();
                var p1 = RandomBytes();
                var p2 = RandomBytes();
                var p3 = RandomBytes();
                var p4 = RandomBytes();

                udf1.Init();
                udf1.Accumulate(p1, Nsq);
                udf1.Accumulate(p2, Nsq);

                udf2.Init();
                udf2.Accumulate(p3, Nsq);
                udf2.Accumulate(p4, Nsq);

                udf3.Init();
                udf3.Accumulate(p1, Nsq);
                udf3.Accumulate(p2, Nsq);
                udf3.Accumulate(p3, Nsq);
                udf3.Accumulate(p4, Nsq);

                udf2.Merge(udf1);

                Assert.Equal(udf2.Terminate().Buffer, udf3.Terminate().Buffer);
            }
        }

        [Fact]
        public void TestMergeEmpty()
        {
            for (var i = 0; i < 50000; i++)
            {
                var udf1 = new PaillierAggregateSum();
                var udf2 = new PaillierAggregateSum();
                var udf3 = new PaillierAggregateSum();

                var Nsq = RandomBytes();
                var p1 = RandomBytes();
                var p2 = RandomBytes();
                var p3 = RandomBytes();

                udf1.Init();
                udf1.Accumulate(p1, Nsq);
                udf1.Accumulate(p2, Nsq);

                udf3.Init();
                udf3.Accumulate(p1, Nsq);
                udf3.Accumulate(p2, Nsq);
                udf3.Accumulate(p3, Nsq);

                udf2.Init();
                udf1.Merge(udf2);

                udf1.Accumulate(p3, Nsq);

                Assert.Equal(udf1.Terminate().Buffer, udf3.Terminate().Buffer);
            }

            for (var i = 0; i < 50000; i++)
            {
                var udf1 = new PaillierAggregateSum();
                var udf2 = new PaillierAggregateSum();
                var udf3 = new PaillierAggregateSum();

                var Nsq = RandomBytes();
                var p1 = RandomBytes();
                var p2 = RandomBytes();
                var p3 = RandomBytes();

                udf1.Init();
                udf1.Accumulate(p1, Nsq);
                udf1.Accumulate(p2, Nsq);

                udf3.Init();
                udf3.Accumulate(p1, Nsq);
                udf3.Accumulate(p2, Nsq);
                udf3.Accumulate(p3, Nsq);

                udf2.Init();
                udf2.Merge(udf1);

                udf2.Accumulate(p3, Nsq);

                Assert.Equal(udf2.Terminate().Buffer, udf3.Terminate().Buffer);
            }
        }

        private SqlBytes RandomBytes()
        {
            var b = new byte[8];
            var bi = BitConverter.GetBytes(rnd.Next());
            Array.Copy(bi, b, bi.Length);
            return new SqlBytes(b);
        }
    }
}
