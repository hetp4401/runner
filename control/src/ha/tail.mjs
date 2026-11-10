// Bounded memory only: entries are never written to D1, SQLite or log files.
export class RuntimeTail {
  constructor({maxBytes=8*1024*1024,maxEntries=2048}={}){this.maxBytes=maxBytes;this.maxEntries=maxEntries;this.entries=[];this.bytes=0;this.sequence=0;this.reports=new Map();this.reportBytes=0;this.transitions=new Map();}
  append(kind,data,at=Date.now()){
    const event={sequence:++this.sequence,at,kind,data};const size=Buffer.byteLength(JSON.stringify(event));
    if(size>this.maxBytes)return null;
    this.entries.push({event,size});this.bytes+=size;
    while(this.bytes>this.maxBytes||this.entries.length>this.maxEntries){this.bytes-=this.entries.shift().size;}
    if(kind==='report'&&typeof data?.run==='string'&&at>=(this.reports.get(data.run)?.at||0)){
      this.reportBytes-=this.reports.get(data.run)?.size||0;
      this.reports.delete(data.run);this.reports.set(data.run,{at,body:data,size});this.reportBytes+=size;
      while(this.reports.size>128||this.reportBytes>this.maxBytes){const key=this.reports.keys().next().value;this.reportBytes-=this.reports.get(key).size;this.reports.delete(key);}
    }
    if(kind==='transition')this.transition(data);
    return event;
  }
  transition(data){if(!data?.id)return;this.transitions.set(data.id,data);while(this.transitions.size>30){const oldest=[...this.transitions].sort((a,b)=>a[1].at-b[1].at)[0][0];this.transitions.delete(oldest);}}
  read(after=0){const first=this.entries[0]?.event.sequence||this.sequence+1;return{sequence:this.sequence,first,gap:after<first-1,events:this.entries.filter(x=>x.event.sequence>after).slice(0,64).map(x=>x.event)};}
  stats(){return{sequence:this.sequence,entries:this.entries.length,bytes:this.bytes,reportBytes:this.reportBytes,transitions:this.transitions.size};}
  history(){return[...this.transitions.values()].sort((a,b)=>b.at-a.at).slice(0,30);}
}
