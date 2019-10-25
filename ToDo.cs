using System;
using Newtonsoft.Json;

namespace change_feed_read_from_beginning
{
    public class ToDo
    {
        [JsonProperty(PropertyName = "id")]
        public string id { get; set; } = Guid.NewGuid().ToString();

        public string Name { get; set; }
        public override string ToString()
        {
            return JsonConvert.SerializeObject(this);
        }

    }
}