#include "Statistics.h"

Statistics::Statistics(){
	this->m_partitionNum = 0;
}

void Statistics::initStatistics() {
		char *relName[] = {(char*)"supplier",(char*)"partsupp", (char*)"lineitem",
					(char*)"orders",(char*)"customer",(char*)"nation", (char*)"part", (char*)"region"};
		AddRel(relName[0],10000);     //supplier
	    AddRel(relName[1],800000);    //partsupp
	    AddRel(relName[2],6001215);   //lineitem
	    AddRel(relName[3],1500000);   //orders
	    AddRel(relName[4],150000);    //customer
	    AddRel(relName[5],25);        //nation
	    AddRel(relName[6], 200000);   //part
	    AddRel(relName[7], 5);        //region


	    AddAtt(relName[0], (char*)"s_suppkey",10000);
	    AddAtt(relName[0], (char*)"s_nationkey",25);
		AddAtt(relName[0], (char*)"s_acctbal",9955);
		AddAtt(relName[0], (char*)"s_name",100000);
		AddAtt(relName[0], (char*)"s_address",100000);
		AddAtt(relName[0], (char*)"s_phone",100000);
		AddAtt(relName[0], (char*)"s_comment",10000);


	    AddAtt(relName[1], (char*)"ps_suppkey", 10000);
	    AddAtt(relName[1], (char*)"ps_partkey", 200000);
	    AddAtt(relName[1], (char*)"ps_availqty", 9999);
	    AddAtt(relName[1], (char*)"ps_supplycost", 99865);
	    AddAtt(relName[1], (char*)"ps_comment", 799123);


	    AddAtt(relName[2], (char*) "l_returnflag",3);
	    AddAtt(relName[2], (char*)"l_discount",11);
	    AddAtt(relName[2], (char*)"l_shipmode",7);
	    AddAtt(relName[2], (char*)"l_orderkey",1500000);
	    AddAtt(relName[2], (char*)"l_receiptdate",0);
	    AddAtt(relName[2], (char*)"l_partkey",200000);
	    AddAtt(relName[2], (char*)"l_suppkey",10000);
	    AddAtt(relName[2], (char*)"l_linenumbe",7);
	    AddAtt(relName[2], (char*)"l_quantity",50);
	    AddAtt(relName[2], (char*)"l_extendedprice",933900);
	    AddAtt(relName[2], (char*)"l_tax",9);
	    AddAtt(relName[2], (char*)"l_linestatus",2);
	    AddAtt(relName[2], (char*)"l_shipdate",2526);
	    AddAtt(relName[2], (char*)"l_commitdate",2466);
	    AddAtt(relName[2], (char*)"l_shipinstruct",4);
	    AddAtt(relName[2], (char*)"l_comment",4501941);


	    AddAtt(relName[3], (char*)"o_custkey",150000);
	    AddAtt(relName[3], (char*)"o_orderkey",1500000);
	    AddAtt(relName[3], (char*)"o_orderdate",2406);
	    AddAtt(relName[3], (char*)"o_totalprice",1464556);
	    AddAtt(relName[3], (char*)"o_orderstatus", 3);
	    AddAtt(relName[3], (char*)"o_orderpriority", 5);
	    AddAtt(relName[3], (char*)"o_clerk", 1000);
	    AddAtt(relName[3], (char*)"o_shippriority", 1);
	    AddAtt(relName[3], (char*)"o_comment", 1478684);


	    AddAtt(relName[4], (char*)"c_custkey",150000);
	    AddAtt(relName[4], (char*)"c_nationkey",25);
	    AddAtt(relName[4], (char*)"c_mktsegment",5);
	    AddAtt(relName[4], (char*)"c_name", 150000);
	    AddAtt(relName[4], (char*)"c_address", 150000);
	    AddAtt(relName[4], (char*)"c_phone", 150000);
	    AddAtt(relName[4], (char*)"c_acctbal", 140187);
	    AddAtt(relName[4], (char*)"c_comment", 149965);

	    AddAtt(relName[5], (char*)"n_nationkey",25);
	    AddAtt(relName[5], (char*)"n_regionkey",5);
	    AddAtt(relName[5], (char*)"n_name",25);
	    AddAtt(relName[5], (char*)"n_comment",25);

	    AddAtt(relName[6], (char*)"p_partkey",200000);
	    AddAtt(relName[6], (char*)"p_size",50);
	    AddAtt(relName[6], (char*)"p_container",40);
	    AddAtt(relName[6], (char*)"p_name", 199996);
		AddAtt(relName[6], (char*)"p_mfgr", 5);
		AddAtt(relName[6], (char*)"p_brand", 25);
		AddAtt(relName[6], (char*)"p_type", 150);
		AddAtt(relName[6], (char*)"p_retailprice", 20899);
		AddAtt(relName[6], (char*)"p_comment", 127459);


	    AddAtt(relName[7], (char*)"r_regionkey",5);
	    AddAtt(relName[7], (char*)"r_name",5);
	    AddAtt(relName[7], (char*)"r_comment",5);


}



Statistics::Statistics(Statistics &copyMe){
	
	tr1::unordered_map <string, Rel_Stat *> *copyRel = copyMe.GetRelStat();
	tr1::unordered_map <string, Rel_Stat *>::iterator relIt;
	for(relIt = copyRel->begin(); relIt != copyRel->end(); relIt++) {
		Rel_Stat *rel = new Rel_Stat(relIt->second);
		this->m_rel_stat.insert(make_pair(relIt->first, rel));
	}

	
	tr1::unordered_map <int, Partition*> *copyPart = copyMe.GetPartition();
	tr1::unordered_map <int, Partition*>::iterator partIt;
	for(partIt=copyPart->begin(); partIt!=copyPart->end(); partIt++) {
		Partition *newP = new Partition(partIt->second);
		int newK = partIt->first;
		this->m_partition[newK] = newP;
	}

	this->m_partitionNum = copyMe.m_partitionNum;

	
	tr1::unordered_map <string, vector<string>*> *copyAttToRel = copyMe.GetAttToRel();
	tr1::unordered_map <string, vector<string>*>::iterator a2rIt ;
	for(a2rIt=copyAttToRel->begin(); a2rIt!=copyAttToRel->end(); a2rIt++) {
		vector<string>::iterator vectIt;
		for(vectIt=a2rIt->second->begin();vectIt!=a2rIt->second->end();vectIt++) {
			a2rIt->second->push_back(*vectIt);
		}
	}
}
Statistics::~Statistics()
{
	tr1::unordered_map <string, Rel_Stat *>::iterator rIt;
	for(rIt=m_rel_stat.begin(); rIt!=m_rel_stat.end(); rIt++) {
		if(rIt->second) delete rIt->second;
	}
	m_rel_stat.clear();

	tr1::unordered_map <string, vector<string>*>::iterator aIt;
	for(aIt=m_att_to_rel.begin(); aIt!=m_att_to_rel.end(); aIt++) {
			if(aIt->second) delete aIt->second;
	}
	m_att_to_rel.clear();

	tr1::unordered_map <int, Partition*>::iterator pIt;
	for(pIt=m_partition.begin(); pIt!=m_partition.end(); pIt++) {
			if(pIt->second) delete pIt->second;
	}
	m_partition.clear();
}

int Statistics::ParseRelation(string name, string &rel) {
	int prefixPos = name.find(".");
	if(prefixPos != string::npos) {
		//s.suppkey
		rel = name.substr(0, prefixPos);
	} else {
		//suppkey
		tr1::unordered_map <string, vector<string>*>::iterator crIt =
				this->m_att_to_rel.find(name);
		if(crIt == this->m_att_to_rel.end() || crIt->second->size()<1) {
			cerr <<"Atts: " <<name << " can not find its relations."<<endl;
			return 0;
		}
		vector<string> *rels = crIt->second;
		if(rels->size() <=0) {
			cerr <<"Atts: "<<name <<" does not belong to any relations!"<<endl;
			return 0;
		}
		if(rels->size()==1) { //this att belong to only one relation
			rel = rels->at(0);
		} else { //this att belong to more than one relations, like s.id, l.id, not happen in TPCH
			cerr <<"Atts: "<<name<<" is ambiguous!"<<endl;
			return 0;
		}
	}
	return 1;
}

void Statistics::AddRel(char *relName, int numTuples)
{
	tr1::unordered_map <string, Rel_Stat *>::iterator it =
			this->m_rel_stat.find(string(relName));
	if(it==this->m_rel_stat.end()) { 
		Rel_Stat *rel = new Rel_Stat;
		rel->numTuples = numTuples;
		rel->numTuplesForWritten = numTuples;
		this->m_rel_stat[string(relName)] =rel ;
	} else  {
		it->second->numTuples = numTuples;
		it->second->numTuplesForWritten = numTuples;
	}

}

void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{
	tr1::unordered_map <string, Rel_Stat *>::iterator relIt = this->m_rel_stat.find(string(relName));
	if(relIt == this->m_rel_stat.end()) { 
		cerr <<"relation " <<relName <<" not found!"<<endl;
		exit(0);
	}
	Rel_Stat *rel = relIt->second;
	string attKey(attName);
	unsigned long int relNumDistinct = (numDistincts==-1)? rel->numTuplesForWritten:numDistincts;
	tr1::unordered_map <string, unsigned long int>::iterator attIt = rel->atts.find(attKey);
	if(attIt==rel->atts.end()) { 
		rel->atts.insert(make_pair(attKey, relNumDistinct));
		
		vector<string> *relNames = new vector<string>;
		relNames->push_back(string(relName));
		this->m_att_to_rel.insert(make_pair(string(attName), relNames));
	} else {
		attIt->second = relNumDistinct;
		
		tr1::unordered_map <string, vector<string>*>::iterator it = this->m_att_to_rel.find(string(attName));
		if(it==this->m_att_to_rel.end()) { 
			vector<string> *relNames = new vector<string>;
			relNames->push_back(string(relName));
			this->m_att_to_rel.insert(make_pair(string(attName), relNames));
		} else
			it->second->push_back(string(relName));
	}
}
void Statistics::CopyRel(char *oldName, char *newName)
{
	tr1::unordered_map <string, Rel_Stat *>::iterator oIt = this->m_rel_stat.find(string(oldName));
	if(oIt == this->m_rel_stat.end()) { 
		cerr <<"relation " <<oldName<< " not found!"<<endl;
		exit(0);
	} else { //found
		Rel_Stat *new_rel = new Rel_Stat(oIt->second);
		this->m_rel_stat.insert(make_pair(string(newName), new_rel));
	}
}
	
void Statistics::Read(char *fromWhere){
	FILE *frp = fopen(fromWhere, "r");
	if(frp == NULL) {
		return;
	}

	
	char relName[200], attName[200];
	unsigned long int numValues;
	tr1::unordered_map <string, vector<string>*>::iterator crIt;
	while (fscanf(frp, "%s", relName) != EOF){
		if (strcmp(relName, "RELATION") == 0){

			fscanf(frp, "%s", relName);
			fscanf(frp, "%lu", &numValues);

			tr1::unordered_map <string, Rel_Stat *>::iterator rIt = this->m_rel_stat.find(string(relName));
			if(rIt == this->m_rel_stat.end())
				this->AddRel(relName, numValues);

			fscanf(frp, "%s", attName);
			while (strcmp(attName, "END") != 0)
			{
				fscanf(frp, "%lu", &numValues);
				this->AddAtt(relName, attName, numValues);
				fscanf(frp, "%s", attName);
			}
		}
	}
	fclose(frp);
}

void Statistics::Write(char *fromWhere){
	FILE * fwp = fopen(fromWhere, "w");
	for(tr1::unordered_map <string, Rel_Stat *>::iterator rIt = this->m_rel_stat.begin();
						rIt!=this->m_rel_stat.end(); rIt++) {
		Rel_Stat *r = rIt->second;
		if(!r->copied) {
			fprintf(fwp, "RELATION\n");
			fprintf(fwp, "%s %lu\n", rIt->first.c_str(), r->numTuplesForWritten);
			for(tr1::unordered_map <string, unsigned long int>::iterator aIt = r->atts.begin(); aIt!=r->atts.end(); aIt++) {
				fprintf(fwp, "%s %lu\n", aIt->first.c_str(), aIt->second);
			}
			fprintf(fwp, "END\n");
		}
	}
	fclose(fwp);
}

void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin){
	unsigned long int result = (unsigned long int) this->Estimate(parseTree, relNames, numToJoin);

	if(this->join_rels.size() == 0) { //---selection, no join---
		tr1::unordered_map <string, Rel_Stat *>::iterator rIt
			= this->m_rel_stat.find(string(relNames[0]));
		if(rIt == this->m_rel_stat.end()) {
			cerr <<"Relation "<<relNames[0]<<" can not be found!"<<endl;
			exit(0);
		}
		Rel_Stat *rel = rIt->second;
		rel->numTuples = result;
		return;
	} 
	else {
		set<int> join_part;
		vector<string> single_rel;
		for(vector<string>::iterator jrIt = this->join_rels.begin(); jrIt!=this->join_rels.end(); jrIt++){
			string relName = *jrIt;
			tr1::unordered_map <string, Rel_Stat *>::iterator rIt = this->m_rel_stat.find(relName);
			Rel_Stat *rel = rIt->second;
			if(rel->partition == -1)
				single_rel.push_back(relName);
			else join_part.insert(rel->partition);
		}

		
		if(single_rel.size() == 0 && join_part.size() == 1) {
			tr1::unordered_map <int, Partition*>::iterator jpIt =
					this->m_partition.find(*join_part.begin());
			Partition *jp = jpIt->second;
			jp->numTuples = result;
		} 
		else { 
			Partition *newPart = new Partition;
			newPart->numTuples = result;
			
			for(vector<string>::iterator rsIt=single_rel.begin(); rsIt!=single_rel.end(); rsIt++) {
				newPart->relations->push_back(*rsIt);
				tr1::unordered_map <string, Rel_Stat *>::iterator rIt = this->m_rel_stat.find(*rsIt);
				Rel_Stat *rel = rIt->second;
				rel->partition = this->m_partitionNum;
			}
			
			for(set<int>::iterator spIt=join_part.begin(); spIt!=join_part.end();spIt++) {
				tr1::unordered_map <int, Partition*>::iterator pIt =
						this->m_partition.find(*spIt);
				if(pIt == this->m_partition.end()) {
					cerr <<"Partition " <<*spIt<<" not found!"<<endl;
					exit(0);
				}
				Partition *op = pIt->second;
				for(vector<string>::iterator roIt=op->relations->begin(); roIt!= op->relations->end(); roIt++) {
					newPart->relations->push_back(*roIt);
					tr1::unordered_map <string, Rel_Stat *>::iterator rIt = this->m_rel_stat.find(*roIt);
					Rel_Stat *rel = rIt->second;
					rel->partition = this->m_partitionNum;
				}
			}
			this->m_partition.insert(make_pair(this->m_partitionNum, newPart));
			this->m_partitionNum++;
		}
	}
}

unsigned long int Statistics::findNumDistinctsWithName(string name, char *relNames[], int numToJoin, bool join) {
	string tableName;
	string attName = name;
	tr1::unordered_map <string, unsigned long int>::iterator aIt;
	int prefixPos = attName.find(".");
	if(prefixPos != string::npos) {
		//s.suppkey
		tr1::unordered_map <string, Rel_Stat *>::iterator rIt;
		tableName = attName.substr(0, prefixPos);
		attName = attName.substr(prefixPos+1);
		rIt = this->m_rel_stat.find(tableName);
		if(rIt == this->m_rel_stat.end()) {
			cerr <<"relation " <<tableName <<" not found!"<<endl;
			exit(0);
		}
		Rel_Stat *rel = rIt->second;
		aIt = rel->atts.find(attName);
		if(aIt == rel->atts.end()) {
			cerr <<"Att: " <<attName <<" not found in relation "<<tableName<<endl;
			exit(0);
		}
		if(join) this->join_rels.push_back(tableName);
	} else {
		//suppkey
		tr1::unordered_map <string, vector<string>*>::iterator crIt =
				this->m_att_to_rel.find(attName);
		if(crIt == this->m_att_to_rel.end() || crIt->second->size()<1) {
			cerr <<"Atts: " <<attName << " can not find its relations."<<endl;
			exit(0);
		}
		vector<string> *rels = crIt->second;
		if(rels->size() <=0) {
			cerr <<"Atts: "<<attName <<" does not belong to any relations!"<<endl;
			exit(0);
		}
		if(rels->size()==1) { //this att belong to only one relation
			Rel_Stat * rel = this->m_rel_stat.find(rels->at(0))->second;
			aIt = rel->atts.find(attName);
			if(aIt == rel->atts.end()) {
				cerr <<"Atts: " <<attName <<" not found in relation "<<tableName<<endl;
				exit(0);
			}
			if(join) this->join_rels.push_back(rels->at(0));
		} else { //this att belong to more than one relations, like s.id, l.id, not happen in TPCH
			for(vector<string>::iterator it = rels->begin(); it!=rels->end(); it++) {
				for(int i=0; i<numToJoin; i++) {
					string r(relNames[i]);
					if(r.compare(*it) == 0) { //match
						Rel_Stat * rel = this->m_rel_stat.find(rels->at(0))->second;
						aIt = rel->atts.find(attName);
						if(aIt == rel->atts.end()) {
							cerr <<"Atts: " <<attName <<" not found in relation "<<tableName<<endl;
							exit(0);
						}
						if(join) this->join_rels.push_back(r);
						return aIt->second;
					}
				}
			}
		}
	}
	return aIt->second;
}

double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin){
	
	this->join_rels.clear();

	if(numToJoin==0) {
		cerr <<"no relation to be estimated!"<<endl;
		return 0;
	}

	double result = 1;

	
	if(!parseTree) {  
		return result; 
	}

	string dummyAtt;
	vector<double> andFactors;
	while(parseTree) {

		OrList *leftOr = parseTree->left;
		parseTree = parseTree->rightAnd;
		vector<unsigned long int> orFactors;
		bool sameAtt = false;
		string pilotAtt = "";
		while(leftOr) {
			ComparisonOp *comOp = leftOr->left;

			leftOr = leftOr->rightOr;
			Operand *left = comOp->left;
			Operand *right = comOp->right;
			unsigned long int num1, num2;
			if(left->code == NAME && right->code == NAME) { 
				num1 = findNumDistinctsWithName(string(left->value), relNames, numToJoin, true);
				num2 = findNumDistinctsWithName(string(right->value), relNames, numToJoin, true);
				num1>=num2 ? (orFactors.push_back(num1)) : (orFactors.push_back(num2));

				continue;
			}
			if(left->code == NAME) {
				if(pilotAtt.compare(left->value) == 0)
					sameAtt=true;
				else
					pilotAtt = left->value;

				if(comOp->code == EQUALS) {
					num1 = findNumDistinctsWithName(string(left->value), relNames, numToJoin, false);
					orFactors.push_back(num1);
				} else
					orFactors.push_back(3);
				dummyAtt = string(left->value);
				continue;
			}
			if(right->code == NAME) {
				if(pilotAtt.compare(right->value) == 0)
					sameAtt=true;
				else pilotAtt = left->value;

				if(comOp->code == EQUALS) {
					num2 = findNumDistinctsWithName(string(right->value), relNames, numToJoin, false);
					orFactors.push_back(num2);
				} else
					orFactors.push_back(3);
				dummyAtt = string(right->value);
				continue;
			}
		}

		if(orFactors.size() < 1) continue;
		double factor=0;
		if(sameAtt) {
			for(vector<unsigned long int>::iterator it=orFactors.begin(); it!=orFactors.end(); it++) {
				unsigned long int f = *it;
				factor += (double)1/f;
			}
		} 
		else {
			factor =1;
			for(vector<unsigned long int>::iterator it=orFactors.begin(); it!=orFactors.end(); it++) {
				unsigned long int f = *it;
				factor *= (1 - (double)1/f);
			}
			factor = 1 - factor;
		}
		if(factor) andFactors.push_back(factor);
	}

	if(this->join_rels.size() == 0) {
		vector<string> *dummyRels = this->m_att_to_rel[dummyAtt];
		Rel_Stat *sr = m_rel_stat[*(dummyRels->begin())];
		if(sr->partition == -1)
			result *= sr->numTuples;
		else {
			Partition *p = this->m_partition[sr->partition];
			if(p->relations->size() > numToJoin) {
				cerr <<"Relations can not be joined!" <<endl;
				exit(0);
			}
			result *= p->numTuples;
		}

	} 
	else {
		int numRels = 0;
		for(vector<string>::iterator jrIt = this->join_rels.begin(); jrIt!=this->join_rels.end(); jrIt++) {
			Rel_Stat *rs = this->m_rel_stat[*jrIt];
			if(rs->partition == -1) {
				result *= rs->numTuples;
				numRels ++;
			} else {
				Partition *p = this->m_partition[rs->partition];
				result *= p->numTuples;
				numRels += p->relations->size();
			}
		}
		if(numRels > numToJoin) {
			cerr <<"Relations can not be joined!"<<endl;
			exit(0);
		}
	}

	for(vector<double>::iterator fit = andFactors.begin(); fit!=andFactors.end(); fit++) {
		result *= *fit;
	}
	return result;
}



