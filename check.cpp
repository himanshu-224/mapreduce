#include<iostream>
#include "Markup.h"
using namespace std;
    int main()
    {
        CMarkup xml;
        bool bSuccess = xml.Load("/home/himanshu/Desktop/BTPCode/chunkMap.xml");
        xml.FindElem("CHUNKMAP");
        xml.IntoElem();
        while ( xml.FindElem("CHUNK") )
        {
            //xml.IntoElem();
            cout<<xml.GetAttrib("Number")<<endl;
            xml.FindElem( "Size" );
            int nQty = atoi( MCD_2PCSZ(xml.GetData()) );
            xml.OutOfElem();
            cout<<nQty<<endl;
        }      
    }
