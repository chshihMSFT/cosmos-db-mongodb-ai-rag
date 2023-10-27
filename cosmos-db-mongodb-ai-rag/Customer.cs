using System;
using System.Collections.Generic;
using System.Text;

namespace cosmos_db_mongodb_ai_rag
{
    internal class Customer
    {
        public String id { get; set; }
        public String title { get; set; }
        public String firstName { get; set; }
        public String lastName { get; set; }
        public String emailAddress { get; set; }
        public String phoneNumber { get; set; }
        public DateTime creationDate { get; set; }
        public float[] dataembedding { get; set; }
        public int tokenlen { get; set; }
        public String timestamp { get; set; }

    }

}