#include<iostream>
#include<fstream>
#include<sstream>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include "Markup.h"

using namespace std;
int main()
{
CMarkup xml;
xml.AddElem( "ORDER");
xml.IntoElem();
xml.AddElem( "ITEM" );
xml.IntoElem();
xml.AddElem( "SN", "132487A-J" );
xml.AddElem( "NAME", "crank casing" );
xml.AddElem( "QTY", "1" );
xml.Save( "Sample.xml" );
//std::string s = string(itoa(10));
//cout<<s;
string str=static_cast<ostringstream*>( &(ostringstream() << 10) )->str();

cout<<str;
}


