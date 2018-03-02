using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Tamir.SharpSsh;
using System.IO;
using System.Xml;
using System.Data;
using System.Data.SqlClient;
using System.Configuration;
using System.Net;
using System.Web;

namespace DemandwareDownload
{
    public class Util
    {
        public static string XMLEncode(string TextToEncode)
        {
            string encodedXml = TextToEncode.Replace("&", "&amp;").Replace("<", "&lt;").Replace(">", "&gt;").Replace("\"", "&quot;").Replace("'", "&apos;");
            string cleanString = "";
            for (int i = 0; i < encodedXml.Length; i++)
            {
                //            if (IsPrintable(encodedXml.Substring(i, 1)))
                //            {
                char[] c = encodedXml.Substring(i, 1).ToCharArray();
                int iAscii = (int)c[0];
                if (iAscii >= 32 && iAscii <= 175)
                    cleanString += encodedXml.Substring(i, 1);
                else
                    System.Diagnostics.Debug.WriteLine(iAscii.ToString());
                //            }
            }

            return cleanString;
        }
    }
    class Program
    {
        //static string logFile = "", monFile = "";
        static string monFile = "";
        static string GetJDEAN8(string dwCustNo, string addrLine1, string addrLine2, string city,
            string state, string zip, string fn, string ln, string email, long ProcessRunID)
        {
            AppSettingsReader appSettingsReader = new AppSettingsReader();
            string custMgrDBURL = (String)appSettingsReader.GetValue("WSCustMgrDBURL", email.GetType());
            //string url = custMgrDBURL + "?action=addcustdb&addr1=" +
            //    HttpUtility.UrlEncode(addrLine1.Substring(0,40)) + "&addr2=" + HttpUtility.UrlEncode(addrLine2.Substring(0, 40)) +
            //    "&city=" + HttpUtility.UrlEncode(city.Substring(0, 35)) + "&state=" + HttpUtility.UrlEncode(state.Substring(0, 2)) +
            //    "&postcd=" + HttpUtility.UrlEncode(zip) + "&fstnm=" + HttpUtility.UrlEncode(fn.Substring(0, 40)) + 
            //    "&lstnm=" + HttpUtility.UrlEncode(ln.Substring(0, 40)) +
            //    "&email=" + HttpUtility.UrlEncode(email) + "&dwcustomerno=" + HttpUtility.UrlEncode(dwCustNo);
            string url = custMgrDBURL + "?action=addcustdb&addr1=" +
                HttpUtility.UrlEncode(addrLine1) + "&addr2=" + HttpUtility.UrlEncode(addrLine2) +
                "&city=" + HttpUtility.UrlEncode(city) + "&state=" + HttpUtility.UrlEncode(state) +
                "&postcd=" + HttpUtility.UrlEncode(zip) + "&fstnm=" + HttpUtility.UrlEncode(fn) +
                "&lstnm=" + HttpUtility.UrlEncode(ln) +
                "&email=" + HttpUtility.UrlEncode(email) + "&dwcustomerno=" + HttpUtility.UrlEncode(dwCustNo);
            HttpWebRequest req = (HttpWebRequest)WebRequest.Create(url);
            req.Method = "GET";
            req.ContentType = "text;encoding='utf-8'";

            HttpWebResponse resp = (HttpWebResponse)req.GetResponse();
            Stream strmResp = resp.GetResponseStream();
            Encoding enc = Encoding.GetEncoding("utf-8");
            StreamReader sr = new StreamReader(strmResp, enc);

            string line = sr.ReadToEnd();
            //File.AppendAllText(logFile, DateTime.Now.ToString() + " - " +
            //"GetJDEAN8 url:  " + url + "-" + line + Environment.NewLine);
            ProcessLogLib.ProcessLog.Insert(ProcessRunID, ProcessLogLib.ProcessLogMessageType.Info, "",
                "GetJDEAN8 url:  " + url + "-" + line);
            XmlDocument xmlDoc = new System.Xml.XmlDocument();
            xmlDoc.LoadXml(line);
            foreach (XmlNode node in xmlDoc.ChildNodes[0].ChildNodes)
            {
                if (node.Name == "Error")
                {
                    if (node.Attributes["ID"].Value.Length > 0)
                    {
                        throw new Exception(node.Attributes["Desc"].Value);
                    }
                }
                if (node.Name == "address_no")
                {
                    return node.Attributes["ID"].Value;
                }
            }
            throw new Exception("Error returned XML from web service CustomerManager.aspx: " + line);
        }
        static string GetVertexGeoCode(string addrLine1, string city, string state, string zip, string country, long ProcessRunID)
        {
            try
            {
                AppSettingsReader appSettingsReader = new AppSettingsReader();
                string geocdURL = (String)appSettingsReader.GetValue("GeoCdURL", zip.GetType());
                string url = geocdURL + "&str=" + HttpUtility.UrlEncode(addrLine1) +
                    "&city=" + HttpUtility.UrlEncode(city) + "&state=" + HttpUtility.UrlEncode(state) +
                    "&postcd=" + HttpUtility.UrlEncode(zip) + "&country=" + country;
                HttpWebRequest req = (HttpWebRequest)WebRequest.Create(url);
                req.Method = "GET";
                req.ContentType = "text;encoding='utf-8'";

                HttpWebResponse resp = (HttpWebResponse)req.GetResponse();
                Stream strmResp = resp.GetResponseStream();
                Encoding enc = Encoding.GetEncoding("utf-8");
                StreamReader sr = new StreamReader(strmResp, enc);

                string line = sr.ReadToEnd();
                //File.AppendAllText(logFile, DateTime.Now.ToString() + " - " +
                //"GetVertexGeocode url:  " + url + "-" + line + Environment.NewLine);
                ProcessLogLib.ProcessLog.Insert(ProcessRunID, ProcessLogLib.ProcessLogMessageType.Info, "",
                    "GetVertexGeocode url:  " + url + "-" + line);
                XmlDocument xmlDoc = new System.Xml.XmlDocument();
                xmlDoc.LoadXml(line);
                foreach (XmlNode node in xmlDoc.ChildNodes[0].ChildNodes)
                {
                    if (node.Name == "Error")
                    {
                        if (node.Attributes["ID"].Value == "-1")
                        {
                            //throw new Exception(node.Attributes["Desc"].Value);
                            return "Error VertexGeocode: " + node.Attributes["Desc"].Value;
                        }
                    }
                    if (node.Name == "GeoCode")
                    {
                        return node.InnerText;
                    }
                }
                //throw new Exception("Error returned XML from web service tax.aspx: " + line);
                return "Error VertexGeocode xml: " + line;
            }
            catch (Exception ex)
            {
                return "Error VertexGeocode: " + ex.Message;
            }
        }
        static void LoadOrders(SqlConnection conn, string dwFile, string xml, bool Recover, long ProcessRunID)
        {
            XmlDocument doc = new XmlDocument();
            doc.LoadXml(xml);
            XmlNode root = doc.DocumentElement;
            string order_no = "", currency_code = "", customer_name = "", customer_email = "", customer_no = "",
                shipping_first_name = "", shipping_last_name = "", shipping_address1 = "", shipping_address2 = "",
                shipping_city = "", shipping_postal_code = "", shipping_state = "", shipping_country = "",
                shipping_phone = "", cc_type = "", cc_number = "", cc_user_name = "", cc_exp_month = "", cc_exp_year = "",
                cc_authorized_amount = "0", billing_first_name = "", billing_last_name = "", billing_address1 = "",
                billing_address2 = "", billing_city = "", billing_postal_code = "", billing_state = "", billing_country = "",
                billing_phone = "", order_status = "", shipping_status = "", confirm_status = "", payment_status = "",
                VertexGeoCode = "", affiliateReferralURL = "", addressBookNo = "", ccEncrypted = "", shipment_id = "",
                shipping_method = "", idStamp = "", approvalCode = "", affiliateAccountNo = "", merch_tot_net_price = "",
                merch_tot_tax = "", merch_tot_gross_price = "", adj_merch_tot_net_price = "", adj_merch_tot_tax = "",
                adj_merch_tot_gross_price = "", shipping_tot_net_price = "", shipping_tot_tax = "", shipping_tot_gross_price = "",
                adj_shipping_tot_net_price = "", adj_shipping_tot_tax = "", adj_shipping_tot_gross_price = "",
                order_tot_net_price = "", order_tot_tax = "", order_tot_gross_price = "", remoteHost = "",
                order_date = "", created_by = "", original_order_no = "", customer_locale = "",
                merch_tot_price_adj_net_price = "", merch_tot_price_adj_tax = "",
                merch_tot_price_adj_gross_price = "", merch_tot_price_adj_base_price = "", merch_tot_price_adj_line_item = "",
                merch_tot_price_adj_promotion_id = "", merch_tot_price_adj_campaign_id = "", merch_tot_price_adj_coupon_id = "",
                giftcertificate_id = "", giftcertificate_amount = "0", giftcertificate_processor_id = "", giftcertificate_transaction_id = "",
                source_code = "", source_code_decrypted = "", gcline_net_price = "", gcline_tax = "", gcline_gross_price = "",
                gcline_base_price = "", gcline_lineitem_text = "", gcline_tax_basis = "", gcline_giftcertificate_id = "",
                gcline_recipient_email = "", gcline_sender_name = "", gcline_recipient_name = "", gcline_message = "", gcline_shipment_id = "", geo_code = "",
                custom_method_name = "", site_origin = "", address_book_no_bill_to = "", address_book_no_ship_to = "", patient_customer_no = "",
                my_doctor_user_id = "", 
                // 11/08/2017 Tien add columns for Stripe
                card_token = "", card_id = "", customer_id = "", transaction_id = "";
            DateTime ordDt = DateTime.MinValue;
            bool acctNoUpdated = false;
            int auditID = 0;
            string pmntType = "";
            XmlNode nodeTotMerchPrAdj = null;
            XmlNode nodeGCLines = null;
            XmlNode nodeOrdGCs = null;
            foreach (XmlNode node in root.ChildNodes)
            {
                if (node.Name == "order")
                {
                    order_no = ""; currency_code = ""; customer_name = ""; customer_email = ""; customer_no = "";
                    shipping_first_name = ""; shipping_last_name = ""; shipping_address1 = ""; shipping_address2 = "";
                    shipping_city = ""; shipping_postal_code = ""; shipping_state = ""; shipping_country = "";
                    shipping_phone = ""; cc_type = ""; cc_number = ""; cc_user_name = ""; cc_exp_month = ""; cc_exp_year = "";
                    cc_authorized_amount = "0"; billing_first_name = ""; billing_last_name = ""; billing_address1 = "";billing_address2 = "";
                    billing_city = ""; billing_postal_code = ""; billing_state = ""; billing_country = "";
                    billing_phone = ""; order_status = ""; shipping_status = ""; confirm_status = ""; payment_status = "";
                    VertexGeoCode = ""; affiliateReferralURL = ""; addressBookNo = ""; ccEncrypted = ""; shipment_id = "";
                    shipping_method = ""; idStamp = ""; approvalCode = ""; affiliateAccountNo = ""; merch_tot_net_price = "";
                    merch_tot_tax = ""; merch_tot_gross_price = ""; merch_tot_net_price = "";
                    merch_tot_tax = ""; merch_tot_gross_price = ""; adj_merch_tot_net_price = ""; adj_merch_tot_tax = "";
                    adj_merch_tot_gross_price = ""; shipping_tot_net_price = ""; shipping_tot_tax = ""; shipping_tot_gross_price = "";
                    adj_shipping_tot_net_price = ""; adj_shipping_tot_tax = ""; adj_shipping_tot_gross_price = "";
                    order_tot_net_price = ""; order_tot_tax = ""; order_tot_gross_price = ""; remoteHost = "";
                    order_date = ""; created_by = ""; original_order_no = ""; customer_locale = "";
                    merch_tot_price_adj_net_price = ""; merch_tot_price_adj_tax = "";
                    giftcertificate_id = ""; giftcertificate_amount = "0"; giftcertificate_processor_id = ""; giftcertificate_transaction_id = "";
                    source_code = ""; source_code_decrypted = ""; geo_code = ""; custom_method_name = ""; site_origin = "";
                    address_book_no_bill_to = ""; address_book_no_ship_to = ""; patient_customer_no = ""; my_doctor_user_id = "";
                    card_token = ""; card_id = ""; customer_id = ""; transaction_id = ""; pmntType = "";
                     
                    ordDt = DateTime.MinValue;
                    acctNoUpdated = false;
                    nodeTotMerchPrAdj = null;
                    nodeGCLines = null;
                    nodeOrdGCs = null;

                    order_no = node.Attributes["order-no"].InnerText;
                    foreach (XmlNode nodeOrder in node.ChildNodes)
                    {
                        if (nodeOrder.Name == "order-date")
                        {
                            order_date = nodeOrder.InnerText;
                            if (!DateTime.TryParse(order_date, out ordDt)) ordDt = DateTime.MinValue;
                        }
                        if (nodeOrder.Name == "created-by")
                            created_by = nodeOrder.InnerText;
                        if (nodeOrder.Name == "original-order-no")
                            original_order_no = nodeOrder.InnerText;
                        if (nodeOrder.Name == "customer-locale")
                            customer_locale = nodeOrder.InnerText;
                        if (nodeOrder.Name == "currency")
                            currency_code = nodeOrder.InnerText;
                        if (nodeOrder.Name == "remoteHost")
                            remoteHost = nodeOrder.InnerText;
                        if (nodeOrder.Name == "source-code")
                        {
                            foreach (XmlNode nodeSrc in nodeOrder.ChildNodes)
                            {
                                if (nodeSrc.Name == "code")
                                {
                                    source_code = nodeSrc.InnerText;
                                }
                            }
                        }
                        if (nodeOrder.Name == "customer")
                        {
                            foreach (XmlNode nodeCust in nodeOrder.ChildNodes)
                            {
                                if (nodeCust.Name == "customer-no")
                                    customer_no = nodeCust.InnerText;
                                if (nodeCust.Name == "customer-name")
                                    customer_name = nodeCust.InnerText;
                                if (nodeCust.Name == "customer-email")
                                    customer_email = nodeCust.InnerText;
                                if (nodeCust.Name == "billing-address")
                                {
                                    foreach (XmlNode nodeBAddr in nodeCust.ChildNodes)
                                    {
                                        if (nodeBAddr.Name == "address1")
                                            billing_address1 = nodeBAddr.InnerText;
                                        if (nodeBAddr.Name == "address2")
                                            billing_address2 = nodeBAddr.InnerText;
                                        if (nodeBAddr.Name == "city")
                                            billing_city = nodeBAddr.InnerText;
                                        if (nodeBAddr.Name == "country-code")
                                            billing_country = nodeBAddr.InnerText;
                                        if (nodeBAddr.Name == "first-name")
                                            billing_first_name = nodeBAddr.InnerText;
                                        if (nodeBAddr.Name == "last-name")
                                            billing_last_name = nodeBAddr.InnerText;
                                        if (nodeBAddr.Name == "phone")
                                            billing_phone = nodeBAddr.InnerText;
                                        if (nodeBAddr.Name == "postal-code")
                                            billing_postal_code = nodeBAddr.InnerText;
                                        if (nodeBAddr.Name == "state-code")
                                            billing_state = nodeBAddr.InnerText;
                                    }
                                }
                            }
                        }
                        if (nodeOrder.Name == "shipments")
                        {
                            foreach (XmlNode nodeShips in nodeOrder.ChildNodes)
                            {
                                if (nodeShips.Name == "shipment")
                                {
                                    shipment_id = nodeShips.Attributes["shipment-id"].InnerText;
                                    foreach (XmlNode nodeShip in nodeShips.ChildNodes)
                                    {
                                        if (nodeShip.Name == "shipping-method")
                                        {
                                            shipping_method = nodeShip.InnerText;
                                        }
                                        if (nodeShip.Name == "shipping-address")
                                        {
                                            foreach (XmlNode nodeSAddr in nodeShip.ChildNodes)
                                            {
                                                if (nodeSAddr.Name == "first-name")
                                                    shipping_first_name = nodeSAddr.InnerText;
                                                if (nodeSAddr.Name == "last-name")
                                                    shipping_last_name = nodeSAddr.InnerText;
                                                if (nodeSAddr.Name == "address1")
                                                    shipping_address1 = nodeSAddr.InnerText;
                                                if (nodeSAddr.Name == "address2")
                                                    shipping_address2 = nodeSAddr.InnerText;
                                                if (nodeSAddr.Name == "city")
                                                    shipping_city = nodeSAddr.InnerText;
                                                if (nodeSAddr.Name == "postal-code")
                                                    shipping_postal_code = nodeSAddr.InnerText;
                                                if (nodeSAddr.Name == "state-code")
                                                    shipping_state = nodeSAddr.InnerText;
                                                if (nodeSAddr.Name == "country-code")
                                                    shipping_country = nodeSAddr.InnerText;
                                                if (nodeSAddr.Name == "phone")
                                                    shipping_phone = nodeSAddr.InnerText;
                                            }
                                        }
                                        if (nodeShip.Name == "custom-attributes")
                                        {
                                            foreach (XmlNode nodeCAAddr in nodeShip.ChildNodes)
                                            {
                                                if (nodeCAAddr.Name == "custom-attribute")
                                                {
                                                    if (nodeCAAddr.Attributes["attribute-id"].Value == "geoCode")
                                                        geo_code = nodeCAAddr.InnerText;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        if (nodeOrder.Name == "payments")
                        {
                            foreach (XmlNode nodePmnts in nodeOrder.ChildNodes)
                            {
                                if (nodePmnts.Name == "payment")
                                {
                                    pmntType = "";
                                    foreach (XmlNode nodePmnt in nodePmnts.ChildNodes)
                                    {
                                        if (nodePmnt.Name == "credit-card")
                                        {
                                            pmntType = "cc";
                                            foreach (XmlNode nodeCC in nodePmnt.ChildNodes)
                                            {
                                                if (nodeCC.Name == "expiration-month")
                                                    cc_exp_month = nodeCC.InnerText;
                                                if (nodeCC.Name == "expiration-year")
                                                    cc_exp_year = nodeCC.InnerText;
                                                if (nodeCC.Name == "card-number")
                                                    cc_number = nodeCC.InnerText;
                                                if (nodeCC.Name == "card-type")
                                                    cc_type = nodeCC.InnerText;
                                                if (nodeCC.Name == "card-holder")
                                                    cc_user_name = nodeCC.InnerText;
                                                if (nodeCC.Name == "card-token")
                                                    card_token = nodeCC.InnerText;
                                                if (nodeCC.Name == "custom-attributes")
                                                {
                                                    foreach (XmlNode nodecccustomattr in nodeCC.ChildNodes)
                                                    {
                                                        if (nodecccustomattr.Name == "custom-attribute")
                                                        {
                                                            XmlAttribute attrcccustom = nodecccustomattr.Attributes["attribute-id"];
                                                            if (attrcccustom != null)
                                                            {
                                                                if (attrcccustom.Value == "stripeCustomerID")
                                                                {
                                                                    customer_id = nodecccustomattr.InnerText;
                                                                }
                                                                if (attrcccustom.Value == "stripeCardID")
                                                                {
                                                                    card_id = nodecccustomattr.InnerText;
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        if (nodePmnt.Name == "gift-certificate")
                                        {
                                            nodeOrdGCs = nodeOrder; // <payments>
                                            pmntType = "gc";
                                        }
                                        if (nodePmnt.Name == "amount")
                                        {
                                            if (pmntType == "cc")
                                                cc_authorized_amount = nodePmnt.InnerText;
                                            if (pmntType == "gc")
                                                giftcertificate_amount = nodePmnt.InnerText;
                                        }
                                        if (nodePmnt.Name == "processor-id")
                                        {
                                            if (pmntType == "cc")
                                                custom_method_name = nodePmnt.InnerText;
                                            if (pmntType == "gc")
                                                giftcertificate_processor_id = nodePmnt.InnerText;
                                        }
                                        if (nodePmnt.Name == "transaction-id")
                                        {
                                            if (pmntType == "cc")
                                                transaction_id = nodePmnt.InnerText;
                                            if (pmntType == "gc")
                                            giftcertificate_transaction_id = nodePmnt.InnerText;
                                        }
                                        if (nodePmnt.Name == "custom-method")
                                        {
                                            foreach (XmlNode nodePmntCustMethod in nodePmnt.ChildNodes)
                                            {
                                                if (nodePmntCustMethod.Name == "method-name")
                                                {
                                                    custom_method_name = nodePmntCustMethod.InnerText;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        if (nodeOrder.Name == "status")
                        {
                            foreach (XmlNode nodeStatus in nodeOrder.ChildNodes)
                            {
                                if (nodeStatus.Name == "order-status")
                                    order_status = nodeStatus.InnerText;
                                if (nodeStatus.Name == "shipping-status")
                                    shipping_status = nodeStatus.InnerText;
                                if (nodeStatus.Name == "confirmation-status")
                                    confirm_status = nodeStatus.InnerText;
                                if (nodeStatus.Name == "payment-status")
                                    payment_status = nodeStatus.InnerText;
                            }
                        }
                        if (nodeOrder.Name == "giftcertificate-lineitems")
                        {
                            nodeGCLines = nodeOrder;
                            nodeGCLines.InnerXml = nodeOrder.InnerXml;
                        }
                        if (nodeOrder.Name == "custom-attributes")
                        {
                            foreach (XmlNode nodeAttr in nodeOrder.ChildNodes)
                            {
                                if (nodeAttr.Name == "custom-attribute")
                                {
                                    switch (nodeAttr.Attributes["attribute-id"].Value)
                                    {
                                        case "CreditCardEncrypted":
                                            ccEncrypted = nodeAttr.InnerText;
                                            break;
                                        case "VertexGeoCode":
                                            VertexGeoCode = nodeAttr.InnerText;
                                            break;
                                        case "AddressBookNumber":
                                            addressBookNo = nodeAttr.InnerText;
                                            break;
                                        case "affiliateAccountNumber":
                                            affiliateAccountNo = nodeAttr.InnerText;
                                            break;
                                        case "ApprovalCode":
                                            approvalCode = nodeAttr.InnerText;
                                            break;
                                        case "IDStamp":
                                            idStamp = nodeAttr.InnerText;
                                            break;
                                        case "affiliateReferralUrl":
                                            affiliateReferralURL = nodeAttr.InnerText;
                                            break;
                                        case "siteorigin":
                                            site_origin = nodeAttr.InnerText;
                                            break;
                                        case "AddressBookNumberBillTo":
                                            address_book_no_bill_to = nodeAttr.InnerText;
                                            break;
                                        case "AddressBookNumberShipTo":
                                            address_book_no_ship_to = nodeAttr.InnerText;
                                            break;
                                        case "patientCustomerNo":
                                            patient_customer_no = nodeAttr.InnerText;
                                            break;
                                        case "myDoctorUserID":
                                            my_doctor_user_id = nodeAttr.InnerText;
                                            break;
                                    }
                                }
                            }
                        }
                        if (nodeOrder.Name == "totals")
                        {
                            foreach (XmlNode nodetots in nodeOrder.ChildNodes)
                            {
                                if (nodetots.Name == "merchandize-total")
                                {
                                    foreach (XmlNode nodetot in nodetots.ChildNodes)
                                    {
                                        if (nodetot.Name == "net-price")
                                        {
                                            merch_tot_net_price = nodetot.InnerText;
                                        }
                                        if (nodetot.Name == "tax")
                                        {
                                            merch_tot_tax = nodetot.InnerText;
                                        }
                                        if (nodetot.Name == "gross-price")
                                        {
                                            merch_tot_gross_price = nodetot.InnerText;
                                        }
                                        if (nodetot.Name == "price-adjustments")
                                        {
                                            nodeTotMerchPrAdj = nodetot;
                                        } 
                                    }
                                }
                                if (nodetots.Name == "adjusted-merchandize-total")
                                {
                                    foreach (XmlNode nodetot in nodetots.ChildNodes)
                                    {
                                        if (nodetot.Name == "net-price")
                                        {
                                            adj_merch_tot_net_price = nodetot.InnerText;
                                        }
                                        if (nodetot.Name == "tax")
                                        {
                                            adj_merch_tot_tax = nodetot.InnerText;
                                        }
                                        if (nodetot.Name == "gross-price")
                                        {
                                            adj_merch_tot_gross_price = nodetot.InnerText;
                                        }
                                    }
                                }
                                if (nodetots.Name == "shipping-total")
                                {
                                    foreach (XmlNode nodetot in nodetots.ChildNodes)
                                    {
                                        if (nodetot.Name == "net-price")
                                        {
                                            shipping_tot_net_price = nodetot.InnerText;
                                        }
                                        if (nodetot.Name == "tax")
                                        {
                                            shipping_tot_tax = nodetot.InnerText;
                                        }
                                        if (nodetot.Name == "gross-price")
                                        {
                                            shipping_tot_gross_price = nodetot.InnerText;
                                        }
                                    }
                                }
                                if (nodetots.Name == "adjusted-shipping-total")
                                {
                                    foreach (XmlNode nodetot in nodetots.ChildNodes)
                                    {
                                        if (nodetot.Name == "net-price")
                                        {
                                            adj_shipping_tot_net_price = nodetot.InnerText;
                                        }
                                        if (nodetot.Name == "tax")
                                        {
                                            adj_shipping_tot_tax = nodetot.InnerText;
                                        }
                                        if (nodetot.Name == "gross-price")
                                        {
                                            adj_shipping_tot_gross_price = nodetot.InnerText;
                                        }
                                    }
                                }
                                if (nodetots.Name == "order-total")
                                {
                                    foreach (XmlNode nodetot in nodetots.ChildNodes)
                                    {
                                        if (nodetot.Name == "net-price")
                                        {
                                            order_tot_net_price = nodetot.InnerText;
                                        }
                                        if (nodetot.Name == "tax")
                                        {
                                            order_tot_tax = nodetot.InnerText;
                                        }
                                        if (nodetot.Name == "gross-price")
                                        {
                                            order_tot_gross_price = nodetot.InnerText;
                                        }
                                    }
                                }
                            }
                        }
                    } // nodeOrder
                } // node.Name == "order"
                // insert into dw_OrderHeader
                try
                {
                    string connStringAudit = ConfigurationManager.ConnectionStrings["AuditLog"].ConnectionString;
                    SqlConnection connAudit = new SqlConnection(connStringAudit);
                    string sqlAudit = "sp_dw_order_audit_insert";
                    SqlCommand cmdAudit = new SqlCommand(sqlAudit, connAudit);
                    if (!Recover)
                    {
                        connAudit.Open();
                        cmdAudit.CommandType = CommandType.StoredProcedure;
                        cmdAudit.Parameters.Add(new SqlParameter("order_no", order_no));
                        cmdAudit.Parameters.Add(new SqlParameter("FileName", dwFile));
                        cmdAudit.Parameters.Add(new SqlParameter("AuditID", SqlDbType.Int, 4, ParameterDirection.Output, false,
                            0, 0, "", DataRowVersion.Default, DBNull.Value));
                        cmdAudit.ExecuteNonQuery();
                        auditID = (int)cmdAudit.Parameters["AuditID"].Value;
                    }
                    if (source_code.Length > 3)
                    {
                        try
                        {
                            WSeBusinessUser.DRMeBusinessUser wsU = new WSeBusinessUser.DRMeBusinessUser();
                            source_code_decrypted = wsU.DecryptCrypto(source_code.Substring(3));
                        }
                        catch (Exception ex)
                        {
                            //File.AppendAllText(logFile, DateTime.Now.ToString() + " - " +
                            //       "Error cannot decrypt source code: " + source_code + " - " +
                            //       ex.Message + Environment.NewLine);
                            if (ex.Message.ToLower().IndexOf("not a valid") < 0)
                            {
                                // ignore invalid encrypted value
                                File.WriteAllText(monFile, "FAIL");
                                ProcessLogLib.ProcessLog.Insert(ProcessRunID, ProcessLogLib.ProcessLogMessageType.Error, "-1",
                                    "Error cannot decrypt source code: " + source_code + " - " +
                                       ex.Message);
                            }
                            else
                            { source_code_decrypted = ""; }
                        }
                    }
                    // 3/8/17 Tien if billing_state = "INTL", override bill to address with ship to address
                    ProcessLogLib.ProcessLog.Insert(ProcessRunID, ProcessLogLib.ProcessLogMessageType.Info, "",
                        "billing address for customer_no=" + customer_no +
                        ", order no=" + order_no + " = " + billing_state + "," + billing_city + "," + billing_address1 + "," + billing_postal_code);
                    if (billing_state.ToUpper() == "INTL")
                    {
                        ProcessLogLib.ProcessLog.Insert(ProcessRunID, ProcessLogLib.ProcessLogMessageType.Info, "",
                            "billing state is INTL, override billing address with shipping address for customer_no=" + customer_no +
                            ", order no=" + order_no);
                        billing_address1 = shipping_address1;
                        billing_address2 = shipping_address2;
                        billing_city = shipping_city;
                        billing_state = shipping_state;
                        billing_postal_code = shipping_postal_code;
                    }
                    // addressBookNo populated?
                    if (addressBookNo.Length == 0)
                    {
                        //File.AppendAllText(logFile, DateTime.Now.ToString() + " - " +
                        //        "no addressBookNo, getting it with bill address for customer_no= " + customer_no + Environment.NewLine);
                        ProcessLogLib.ProcessLog.Insert(ProcessRunID, ProcessLogLib.ProcessLogMessageType.Info, "",
                            "no addressBookNo, getting it with bill address for customer_no= " + customer_no);
                        addressBookNo = GetJDEAN8(customer_no, billing_address1, "", billing_city, billing_state,
                            billing_postal_code, billing_first_name, billing_last_name, customer_email, ProcessRunID);
                        acctNoUpdated = true;
                        //File.AppendAllText(logFile, DateTime.Now.ToString() + " - " +
                        //        "got addressBookNo for customer_no= " + customer_no + ", addressBookNo= " + addressBookNo);
                        ProcessLogLib.ProcessLog.Insert(ProcessRunID, ProcessLogLib.ProcessLogMessageType.Info, "",
                            "got addressBookNo for customer_no= " + customer_no + ", addressBookNo= " + addressBookNo);
                    }
                    // VertexGeocode populated?
                    if (VertexGeoCode.Length == 0) VertexGeoCode = geo_code;
                    if (VertexGeoCode.Length == 0)
                    {
                        //if (billing_city != shipping_city || billing_state != shipping_state || billing_postal_code != shipping_postal_code)
                        //{
                        //    File.AppendAllText(logFile, DateTime.Now.ToString() + " - " +
                        //            "shipping city/state/zip no same as billing city/state/zip, get VerTexGeocode" + Environment.NewLine);
                            // get Vertexgeocode from shipping address
                            string geocd = GetVertexGeoCode(shipping_address1, shipping_city, shipping_state, shipping_postal_code, "US", ProcessRunID);
                            if (geocd.Length > 0)
                            {
                                if (geocd.IndexOf("Error:") > -1)
                                {
                                    //File.AppendAllText(logFile, DateTime.Now.ToString() + " - " +
                                    //       geocd + Environment.NewLine);
                                    File.WriteAllText(monFile, "FAIL");
                                    ProcessLogLib.ProcessLog.Insert(ProcessRunID, ProcessLogLib.ProcessLogMessageType.Error, "-1",
                                        geocd);
                                }
                                else
                                    {
                                        VertexGeoCode = geocd;
                                    }
                             }
                        //}
                    }
                    // insert order header
                    string sql = "insert into dw_OrderHeader([order_no] " +
                   ",[currency_code]" +
                   ",[customer_name]" +
                   ",[customer_email]" +
                   ",[shipping_first_name]" +
                   ",[shipping_last_name]" +
                   ",[shipping_address1]" +
                   ",[shipping_address2]" +
                   ",[shipping_city]" +
                   ",[shipping_postal_code]" +
                   ",[shipping_state]" +
                   ",[shipping_country]" +
                   ",[shipping_phone]" +
                   ",[cc_type]" +
                   ",[cc_number]" +
                   ",[cc_user_name]" +
                   ",[cc_exp_month]" +
                   ",[cc_exp_year]" +
                   ",[cc_authorized_amount]" +
                   ",[update_date]" +
                   ",[updated_by]" +
                   ",[insert_date]" +
                   ",[inserted_by]" +
                   ",[processed]" +
                   ",[billing_first_name]" +
                   ",[billing_last_name]" +
                   ",[billing_address1]" +
                   ",[billing_address2]" +
                   ",[billing_city]" +
                   ",[billing_postal_code]" +
                   ",[billing_state]" +
                   ",[billing_country]" +
                   ",[billing_phone]" +
                   ",[order_status]" +
                   ",[shipping_status]" +
                   ",[confirmation_status]" +
                   ",[payment_status]" +
                   ",[vertex_tax_code]" +
                   ",[order_site]" +
                   ",[account_number]" +
                   ",[shipment_id]" +
                   ",[shipping_method_id]" +
                   ",[cc_IDStamp]" +
                   ",[cc_ApprovalCode]" +
                   ",[affiliate_account_no]" +
                   ",[merchandise_total_net_price]" +
                   ",[merchandise_total_tax]" +
                   ",[merchandise_total_gross_price]" +
                   ",[adj_merchandise_total_net_price]" +
                   ",[adj_merchandise_total_tax]" +
                   ",[adj_merchandise_total_gross_price]" +
                   ",[shipping_total_net_price]" +
                   ",[shipping_total_tax]" +
                   ",[shipping_total_gross_price]" +
                   ",[adj_shipping_total_net_price]" +
                   ",[adj_shipping_total_tax]" +
                   ",[adj_shipping_total_gross_price]" +
                   ",[order_total_net_price]" +
                   ",[order_total_tax]" +
                   ",[order_gross_price]" +
                   ",[affiliateReferralURL]" +
                   ",[customer_no]" +
                   ",[acct_no_updated]" +
                   ",[cc_masked]" +
                   ",[remoteHost]" +
                   ",[order_date]" +
                   ",[created_by]" +
                   ",[original_order_no]" +
                   ",[customer_locale]" +
                   ",[FileName]" +
                   ",[order_type]" +
                   ",[giftcertificate_id]" +
                   ",[giftcertificate_amount]" +
                   ",[giftcertificate_processer_id]" +
                   ",[giftcertificate_transaction_id]" +
                   ",[affiliate_banner_AN8]" +
                   ",[PaymentMethod]" +
                   ",[ShipToAN8]" +
                   ",[DWPatientCustNo]" +
                   ",[card_token]" +
                   ",[card_id]" +
                   ",[customer_id]" +
                   ",[transaction_id]" +
                   ")" +
                   " values ('" +
                    order_no + "','" +
                    currency_code + "','" +
                    customer_name.Replace("'", "''") + "','" +
                    customer_email.Replace("'", "''") + "','" +
                    shipping_first_name.Replace("'", "''") + "','" +
                    shipping_last_name.Replace("'", "''") + "','" +
                    shipping_address1.Replace("'", "''") + "','" +
                    shipping_address2.Replace("'", "''") + "','" +
                    shipping_city.Replace("'", "''") + "','" +
                    shipping_postal_code.Replace("'", "''") + "','" +
                    shipping_state + "','" +
                    shipping_country.Replace("'", "''") + "','" +
                    shipping_phone.Replace("'", "''") + "','" +
                    cc_type + "','" +
                        //cc_number + "','" +
                    ccEncrypted + "','" +
                    cc_user_name.Replace("'", "''") + "','" +
                    cc_exp_month + "','" +
                    cc_exp_year + "'," +
                    cc_authorized_amount + ", getdate()" +
                    ",'SYSTEM'" +
                     ",getdate()" +
                    ",'SYSTEM'" +
                    ",0, '" +
                    billing_first_name.Replace("'", "''") + "','" +
                    billing_last_name.Replace("'", "''") + "','" +
                    billing_address1.Replace("'", "''") + "','" +
                    billing_address2.Replace("'", "''") + "','" +
                    billing_city.Replace("'", "''") + "','" +
                    billing_postal_code + "','" +
                    billing_state + "','" +
                    billing_country + "','" +
                    billing_phone.Replace("'", "''") + "','" +
                   order_status.Replace("'", "''") + "','" +
                   shipping_status.Replace("'", "''") + "','" +
                   confirm_status.Replace("'", "''") + "','" +
                   payment_status.Replace("'", "''") + "','" +
                   VertexGeoCode.Replace("'", "''") + "','" +
                   (dwFile.IndexOf("OrdersCS_") > -1?"ClearStart":"Dermalogica") + "','" +
                   ((site_origin == "SJ" || site_origin == "SD")?address_book_no_bill_to: addressBookNo.Replace("'", "''")) + "','" +
                   shipment_id + "','" +
                   shipping_method.Replace("'", "''") + "','" +
                   idStamp + "','" +
                   approvalCode + "','" +
                   affiliateAccountNo.Replace("'", "''") + "'," +
                   merch_tot_net_price + "," +
                   merch_tot_tax + "," +
                   merch_tot_gross_price + "," +
                   adj_merch_tot_net_price + "," +
                   adj_merch_tot_tax + "," +
                   adj_merch_tot_gross_price + "," +
                   shipping_tot_net_price + "," +
                   shipping_tot_tax + "," +
                   shipping_tot_gross_price + "," +
                   adj_shipping_tot_net_price + "," +
                   adj_shipping_tot_tax + "," +
                   adj_shipping_tot_gross_price + "," +
                   order_tot_net_price + "," +
                   order_tot_tax + "," +
                   order_tot_gross_price + ",'" +
                   affiliateReferralURL.Replace("'", "''") + "','" +
                   customer_no.Replace("'", "''") + "'," +
                   (acctNoUpdated?"1":"0") + ",'" +
                   cc_number + "','" +
                   remoteHost.Replace("'", "''") + "','" +
                   ordDt.ToString() + "','" +
                   created_by + "','" +
                   original_order_no.Replace("'", "''") + "','" +
                   customer_locale.Replace("'", "''") + "','" +
                   dwFile.Replace("'", "''") + "','" +
                   ((site_origin == "SD" || site_origin == "SJ")?site_origin:(dwFile.IndexOf("OrdersCS_") > -1?"SR":"SM")) + "','" +
                   giftcertificate_id.Replace("'", "''") + "'," +
                   giftcertificate_amount + ",'" +
                   giftcertificate_processor_id.Replace("'", "''") + "','" +
                   giftcertificate_transaction_id.Replace("'", "''") + "','" +
                   ((site_origin == "SJ" || site_origin == "SD")? my_doctor_user_id: source_code_decrypted.Replace("'", "''")) + "','" +
                   custom_method_name + "','" +
                   address_book_no_ship_to + "','" +
                   patient_customer_no + "','" +
                   card_token.Replace("'", "''") + "','" +
                   card_id.Replace("'", "''") + "','" +
                   customer_id.Replace("'", "''") + "','" +
                   transaction_id.Replace("'", "''") + 
                    "')";
                   // File.AppendAllText(logFile, DateTime.Now.ToString() + " - " +
                   //         "Insert into dw_OrderHeader: " + sql + Environment.NewLine);
                    ProcessLogLib.ProcessLog.Insert(ProcessRunID, ProcessLogLib.ProcessLogMessageType.Info, "",
                        "Insert into dw_OrderHeader: " + sql);
                    SqlCommand cmd = new SqlCommand(sql, conn);
                    cmd.CommandType = CommandType.Text;
                    cmd.ExecuteNonQuery();

                    // order level gift certificate
                    if (nodeOrdGCs != null)
                    { // nodeOrdGCs is <payments>
                        foreach (XmlNode nodeOrdGC in nodeOrdGCs.ChildNodes)
                        {
                            if (nodeOrdGC.Name == "payment")
                            {
                                foreach (XmlNode nodePayGC in nodeOrdGC.ChildNodes)
                                {
                                    if (nodePayGC.Name == "gift-certificate")
                                    {
                                        foreach (XmlNode nodeGC in nodePayGC.ChildNodes)
                                        {
                                            if (nodeGC.Name == "giftcertificate-id")
                                            {
                                                giftcertificate_id = nodeGC.InnerText;
                                            }
                                        }
                                    }
                                    if (nodePayGC.Name == "amount")
                                    {
                                        giftcertificate_amount = nodePayGC.InnerText;
                                    }
                                    if (nodePayGC.Name == "processor-id")
                                    {
                                        giftcertificate_processor_id = nodePayGC.InnerText;
                                    }
                                    if (nodePayGC.Name == "transaction-id")
                                    {
                                        giftcertificate_transaction_id = nodePayGC.InnerText;
                                    }
                                    if (nodePayGC.Name == "custom-method")
                                    {
                                        foreach (XmlNode nodePmntCustMethod in nodePayGC.ChildNodes)
                                        {
                                            if (nodePmntCustMethod.Name == "method-name")
                                            {
                                                custom_method_name = nodePmntCustMethod.InnerText;
                                            }
                                        }
                                    }
                                }
                                sql = "insert into dw_OrderHeaderGC(" +
                                            "[order_no], [giftcertificate_id], [giftcertificate_amount], [giftcertificate_processer_id], " +
                                            "[giftcertificate_transaction_id]) " +
                                        " values('" + order_no + "','" + giftcertificate_id + "','" + giftcertificate_amount + "','" +
                                                    giftcertificate_processor_id + "','" + giftcertificate_transaction_id +
                                        "')";
                                    cmd = new SqlCommand(sql, conn);
                                    cmd.CommandType = CommandType.Text;
                                    cmd.ExecuteNonQuery();
                                    ProcessLogLib.ProcessLog.Insert(ProcessRunID, ProcessLogLib.ProcessLogMessageType.Info, "",
                                        "Insert into dw_OrderHeaderGC: " + sql);
                            }
                        }
                    }
                    // total merchandise price adjustments
                    if (nodeTotMerchPrAdj != null)
                    {
                        foreach (XmlNode nodeMerchTotPrAdjs in nodeTotMerchPrAdj.ChildNodes)
                        {
                            if (nodeMerchTotPrAdjs.Name == "price-adjustment")
                            {
                                merch_tot_price_adj_gross_price = ""; merch_tot_price_adj_base_price = ""; merch_tot_price_adj_line_item = "";
                                merch_tot_price_adj_promotion_id = ""; merch_tot_price_adj_campaign_id = ""; merch_tot_price_adj_coupon_id = "";
                                foreach (XmlNode nodeMerchTotPrAdj in nodeMerchTotPrAdjs.ChildNodes)
                                {
                                    if (nodeMerchTotPrAdj.Name == "net-price")
                                    {
                                        merch_tot_price_adj_net_price = nodeMerchTotPrAdj.InnerText;
                                    }
                                    if (nodeMerchTotPrAdj.Name == "tax")
                                    {
                                        merch_tot_price_adj_tax = nodeMerchTotPrAdj.InnerText;
                                    }
                                    if (nodeMerchTotPrAdj.Name == "gross-price")
                                    {
                                        merch_tot_price_adj_gross_price = nodeMerchTotPrAdj.InnerText;
                                    }
                                    if (nodeMerchTotPrAdj.Name == "base-price")
                                    {
                                        merch_tot_price_adj_base_price = nodeMerchTotPrAdj.InnerText;
                                    }
                                    if (nodeMerchTotPrAdj.Name == "line-item")
                                    {
                                        merch_tot_price_adj_line_item = nodeMerchTotPrAdj.InnerText;
                                    }
                                    if (nodeMerchTotPrAdj.Name == "promotion-id")
                                    {
                                        merch_tot_price_adj_promotion_id = nodeMerchTotPrAdj.InnerText;
                                    }
                                    if (nodeMerchTotPrAdj.Name == "campaign-id")
                                    {
                                        merch_tot_price_adj_campaign_id = nodeMerchTotPrAdj.InnerText;
                                    }
                                    if (nodeMerchTotPrAdj.Name == "coupon-id")
                                    {
                                        merch_tot_price_adj_coupon_id = nodeMerchTotPrAdj.InnerText;
                                    }
                                } // nodeMerchTotPrAdjs.ChildNodes
                                // insert into db
                                decimal merchTotPrAdjNetPrice = 0;
                                if (!decimal.TryParse(merch_tot_price_adj_net_price, out merchTotPrAdjNetPrice))
                                    merchTotPrAdjNetPrice = 0;
                                decimal merchTotPrAdjGrossPrice = 0;
                                if (!decimal.TryParse(merch_tot_price_adj_gross_price, out merchTotPrAdjGrossPrice))
                                    merchTotPrAdjGrossPrice = 0;
                                decimal merchTotPrAdjTax = 0;
                                if (!decimal.TryParse(merch_tot_price_adj_tax, out merchTotPrAdjTax))
                                    merchTotPrAdjTax = 0;
                                decimal merchTotPrAdjBasePrice = 0;
                                if (!decimal.TryParse(merch_tot_price_adj_base_price, out merchTotPrAdjBasePrice))
                                    merchTotPrAdjBasePrice = 0;
                                string sqlOrdPrAdj = "dw_OrderHeader_PriceAdj_insertupdate";
                                SqlCommand cmdOrdPrAdj = new SqlCommand(sqlOrdPrAdj, conn);
                                cmdOrdPrAdj.CommandType = CommandType.StoredProcedure;
                                cmdOrdPrAdj.Parameters.Add(new SqlParameter("order_no", order_no));
                                cmdOrdPrAdj.Parameters.Add(new SqlParameter("net_price", merchTotPrAdjNetPrice));
                                cmdOrdPrAdj.Parameters.Add(new SqlParameter("tax", merchTotPrAdjTax));
                                cmdOrdPrAdj.Parameters.Add(new SqlParameter("gross_price", merchTotPrAdjGrossPrice));
                                cmdOrdPrAdj.Parameters.Add(new SqlParameter("base_price", merchTotPrAdjBasePrice));
                                cmdOrdPrAdj.Parameters.Add(new SqlParameter("line_item", merch_tot_price_adj_line_item));
                                cmdOrdPrAdj.Parameters.Add(new SqlParameter("promotion_id", merch_tot_price_adj_promotion_id));
                                cmdOrdPrAdj.Parameters.Add(new SqlParameter("campaign_id", merch_tot_price_adj_campaign_id));
                                cmdOrdPrAdj.Parameters.Add(new SqlParameter("coupon_id", merch_tot_price_adj_coupon_id));
                                cmdOrdPrAdj.Parameters.Add(new SqlParameter("user_id", "SYSTEM"));
                                cmdOrdPrAdj.ExecuteNonQuery();
                                //File.AppendAllText(logFile, DateTime.Now.ToString() + " - " +
                                //        "Insert into dw_OrderHeader_PriceAdj: " + sqlOrdPrAdj + Environment.NewLine);
                                ProcessLogLib.ProcessLog.Insert(ProcessRunID, ProcessLogLib.ProcessLogMessageType.Info, "",
                                    "Insert into dw_OrderHeader_PriceAdj: " + sqlOrdPrAdj);
                            } // if ( nodeMerchTotPrAdjs.Name == "price_adjustment"
                        }
                    }
                    if (nodeGCLines != null)
                    {
                        foreach (XmlNode nodegcline in nodeGCLines.ChildNodes)
                        {
                            foreach (XmlNode nodegc in nodegcline)
                            {
                                if (nodegc.Name == "net-price")
                                    gcline_net_price = nodegc.InnerText;
                                if (nodegc.Name == "tax")
                                    gcline_tax = nodegc.InnerText;
                                if (nodegc.Name == "gross-price")
                                    gcline_gross_price = nodegc.InnerText;
                                if (nodegc.Name == "base-price")
                                    gcline_base_price = nodegc.InnerText;
                                if (nodegc.Name == "lineitem-text")
                                    gcline_lineitem_text = nodegc.InnerText;
                                if (nodegc.Name == "tax-basis")
                                    gcline_tax_basis = nodegc.InnerText;
                                if (nodegc.Name == "giftcertificate-id")
                                    gcline_giftcertificate_id = nodegc.InnerText;
                                if (nodegc.Name == "recipient-email")
                                    gcline_recipient_email = nodegc.InnerText;
                                if (nodegc.Name == "sender-name")
                                    gcline_sender_name = nodegc.InnerText;
                                if (nodegc.Name == "recipient-name")
                                    gcline_recipient_name = nodegc.InnerText;
                                if (nodegc.Name == "message")
                                    gcline_message = nodegc.InnerText;
                                if (nodegc.Name == "shipment-id")
                                    gcline_shipment_id = nodegc.InnerText;

                            }
                            string sqlgc = "dw_OrderLineGC_insertupdate";
                            SqlCommand cmdgc = new SqlCommand(sqlgc, conn);
                            cmdgc.CommandType = CommandType.StoredProcedure;
                            cmdgc.Parameters.Add(new SqlParameter("order_line_gc_id", SqlDbType.Int, 4, ParameterDirection.InputOutput, false,
                                0, 0, "order_line_gc_id", DataRowVersion.Default, DBNull.Value));
                            cmdgc.Parameters.Add(new SqlParameter("order_no", order_no));
                            cmdgc.Parameters.Add(new SqlParameter("net_price", decimal.Parse(gcline_net_price)));
                            cmdgc.Parameters.Add(new SqlParameter("tax", decimal.Parse(gcline_tax)));
                            cmdgc.Parameters.Add(new SqlParameter("gross_price", decimal.Parse(gcline_gross_price)));
                            cmdgc.Parameters.Add(new SqlParameter("base_price", decimal.Parse(gcline_base_price)));
                            cmdgc.Parameters.Add(new SqlParameter("line_item", gcline_lineitem_text));
                            cmdgc.Parameters.Add(new SqlParameter("tax_basis", decimal.Parse(gcline_tax_basis)));
                            cmdgc.Parameters.Add(new SqlParameter("gc_id", gcline_giftcertificate_id));
                            cmdgc.Parameters.Add(new SqlParameter("recipient_email", gcline_recipient_email));
                            cmdgc.Parameters.Add(new SqlParameter("sender_name", gcline_sender_name));
                            cmdgc.Parameters.Add(new SqlParameter("recipient_name", gcline_recipient_name));
                            cmdgc.Parameters.Add(new SqlParameter("message", gcline_message));
                            cmdgc.Parameters.Add(new SqlParameter("shipment_id", gcline_shipment_id));
                            cmdgc.Parameters.Add(new SqlParameter("shipping_status", DBNull.Value));
                            cmdgc.ExecuteNonQuery();
                            //File.AppendAllText(logFile, DateTime.Now.ToString() + " - " +
                            //        "Insert into dw_OrderLineGC_insertupdate: " + sqlgc + Environment.NewLine);
                            ProcessLogLib.ProcessLog.Insert(ProcessRunID, ProcessLogLib.ProcessLogMessageType.Info, "",
                                "Insert into dw_OrderLineGC_insertupdate: " + sqlgc);
                        }
                    }

                    // demandwareJDE_Freight_Xref
                    sql = "insert into DemandWareJDE_Freight_Xref(Item_ID, Shipment_ID, CARS, FRTH, ROUT) " +
                        " values('" + shipping_status + "','" + shipment_id + "',0,'','')";
                    cmd = new SqlCommand(sql, conn);
                    cmd.CommandType = CommandType.Text;
                    cmd.ExecuteNonQuery();
                    //File.AppendAllText(logFile, DateTime.Now.ToString() + " - " +
                    //        "Insert into DemandWareJDE_Freight_Xref: " + sql + Environment.NewLine);
                    ProcessLogLib.ProcessLog.Insert(ProcessRunID, ProcessLogLib.ProcessLogMessageType.Info, "",
                        "Insert into DemandWareJDE_Freight_Xref: " + sql);

                    // insert dw_OrderLIneItem
                    // here node.name is still Order
                    foreach (XmlNode nodeOrderSub in node.ChildNodes)
                    {
                        if (nodeOrderSub.Name == "product-lineitems")
                        {
                            string unit_price = "", tax = "", line_item = "", product_id = "", product_name = "",
                                quantity = "", tax_rate = "", quantity_unit = "";
                            foreach (XmlNode nodeItems in nodeOrderSub.ChildNodes)
                            {
                                if (nodeItems.Name == "product-lineitem")
                                {
                                    XmlNode nodeLinePrAdjs = null;
                                    foreach (XmlNode nodeItem in nodeItems.ChildNodes)
                                    {
                                        if (nodeItem.Name == "net-price")
                                            unit_price = nodeItem.InnerText;
                                        if (nodeItem.Name == "tax")
                                            tax = nodeItem.InnerText;
                                        if (nodeItem.Name == "lineitem-text")
                                            line_item = nodeItem.InnerText;
                                        if (nodeItem.Name == "product-name")
                                            product_name = nodeItem.InnerText;
                                        if (nodeItem.Name == "quantity")
                                        {
                                            quantity_unit = nodeItem.Attributes["unit"].Value;
                                            quantity = nodeItem.InnerText;
                                        }
                                        if (nodeItem.Name == "tax-rate")
                                            tax_rate = nodeItem.InnerText;
                                        if (nodeItem.Name == "custom-attributes")
                                        {
                                            foreach (XmlNode nodeAttr in nodeItem.ChildNodes)
                                            {
                                                if (nodeAttr.Attributes["attribute-id"].Value == "SKU")
                                                    product_id = nodeAttr.InnerText;
                                            }
                                        }
                                        if (nodeItem.Name == "price-adjustments")
                                        {
                                            nodeLinePrAdjs = nodeItem;
                                        }
                                    }
                                    decimal unitprice = 0;
                                    if (!decimal.TryParse(unit_price, out unitprice)) unitprice = 0;
                                    decimal taxamt = 0;
                                    if (!decimal.TryParse(tax, out taxamt)) taxamt = 0;
                                    decimal quantityamt = 0;
                                    if (!decimal.TryParse(quantity, out quantityamt)) quantityamt = 0;
                                    decimal taxrate = 0;
                                    if (!decimal.TryParse(tax_rate, out taxrate)) taxrate = 0;

                                    // insert into dw_OrderLineItem
                                    sql = "dw_OrderLineItem_insertupdate";
                                    cmd = new SqlCommand(sql, conn);
                                    cmd.CommandType = CommandType.StoredProcedure;
                                    cmd.Parameters.Add(new SqlParameter("order_detail_id", SqlDbType.Int, 4, ParameterDirection.InputOutput, false, 0, 0,
                                        "order_detail_id", DataRowVersion.Default, DBNull.Value));
                                    cmd.Parameters.Add(new SqlParameter("order_no", order_no));
                                    cmd.Parameters.Add(new SqlParameter("unit_price", unitprice));
                                    cmd.Parameters.Add(new SqlParameter("tax", taxamt));
                                    cmd.Parameters.Add(new SqlParameter("line_item", line_item));
                                    cmd.Parameters.Add(new SqlParameter("product_id", product_id));
                                    cmd.Parameters.Add(new SqlParameter("product_name", product_name));
                                    cmd.Parameters.Add(new SqlParameter("quantity_unit", quantityamt));
                                    cmd.Parameters.Add(new SqlParameter("tax_rate", taxrate));
                                    cmd.Parameters.Add(new SqlParameter("vertex_tax_rate", DBNull.Value));
                                    cmd.Parameters.Add(new SqlParameter("hello_product_id", product_id));
                                    cmd.Parameters.Add(new SqlParameter("shipment_id", shipment_id));
                                    cmd.Parameters.Add(new SqlParameter("shipping_status", shipping_status));
                                    cmd.ExecuteNonQuery();
                                    //File.AppendAllText(logFile, DateTime.Now.ToString() + " - " +
                                    //        "dw_OrderLineItem_insertupdate " + Environment.NewLine);
                                    ProcessLogLib.ProcessLog.Insert(ProcessRunID, ProcessLogLib.ProcessLogMessageType.Info, "",
                                        "dw_OrderLineItem_insertupdate");
                                    int orderDetID = (int)cmd.Parameters["order_detail_id"].Value;
                                    // insert into dw_OrderLineItem_PriceAdj
                                    if (nodeLinePrAdjs != null)
                                    {
                                        foreach (XmlNode nodepradj in nodeLinePrAdjs.ChildNodes)
                                        {
                                            if (nodepradj.Name == "price-adjustment")
                                            {
                                                string line_pr_adj_net_price = "", line_pr_adj_tax = "", line_pr_adj_gross_price = "",
                                                    line_pr_adj_base_price = "", line_pr_adj_lineitem_text = "",
                                                    line_pr_adj_promotion_id = "", line_pr_adj_campaign_id = "",
                                                    line_pr_adj_coupon_id = "";
                                                foreach (XmlNode nodepradjsub in nodepradj.ChildNodes)
                                                {
                                                    if (nodepradjsub.Name == "net-price")
                                                        line_pr_adj_net_price = nodepradjsub.InnerText;
                                                    if (nodepradjsub.Name == "tax")
                                                        line_pr_adj_tax = nodepradjsub.InnerText;
                                                    if (nodepradjsub.Name == "gross-price")
                                                        line_pr_adj_gross_price = nodepradjsub.InnerText;
                                                    if (nodepradjsub.Name == "base-price")
                                                        line_pr_adj_base_price = nodepradjsub.InnerText;
                                                    if (nodepradjsub.Name == "lineitem-text")
                                                        line_pr_adj_lineitem_text = nodepradjsub.InnerText;
                                                    if (nodepradjsub.Name == "promotion-id")
                                                        line_pr_adj_promotion_id = nodepradjsub.InnerText;
                                                    if (nodepradjsub.Name == "campaign-id")
                                                        line_pr_adj_campaign_id = nodepradjsub.InnerText;
                                                    if (nodepradjsub.Name == "coupon-id")
                                                        line_pr_adj_coupon_id = nodepradjsub.InnerText;
                                                }
                                                decimal linepradjnetprice = 0;
                                                if (!decimal.TryParse(line_pr_adj_net_price, out linepradjnetprice)) linepradjnetprice = 0;
                                                decimal linepradjgrossprice = 0;
                                                if (!decimal.TryParse(line_pr_adj_gross_price, out linepradjgrossprice)) linepradjgrossprice = 0;
                                                decimal linepradjbaseprice = 0;
                                                if (!decimal.TryParse(line_pr_adj_base_price, out linepradjbaseprice)) linepradjbaseprice = 0;
                                                decimal linepradjtax = 0;
                                                if (!decimal.TryParse(line_pr_adj_tax, out linepradjtax)) linepradjtax = 0;
                                                // insert into dw_OrderLineItem_priceAdj
                                                sql = "dw_OrderLineItem_PriceAdj_insertupdate";
                                                cmd = new SqlCommand(sql, conn);
                                                cmd.CommandType = CommandType.StoredProcedure;
                                                cmd.Parameters.Add(new SqlParameter("order_detail_id", orderDetID));
                                                cmd.Parameters.Add(new SqlParameter("order_no", order_no));
                                                cmd.Parameters.Add(new SqlParameter("product_id", product_id));
                                                cmd.Parameters.Add(new SqlParameter("net_price", linepradjnetprice));
                                                cmd.Parameters.Add(new SqlParameter("tax", linepradjtax));
                                                cmd.Parameters.Add(new SqlParameter("gross_price", linepradjgrossprice));
                                                cmd.Parameters.Add(new SqlParameter("base_price", linepradjbaseprice));
                                                cmd.Parameters.Add(new SqlParameter("line_item", line_pr_adj_lineitem_text));
                                                cmd.Parameters.Add(new SqlParameter("promotion_id", line_pr_adj_promotion_id));
                                                cmd.Parameters.Add(new SqlParameter("campaign_id", line_pr_adj_campaign_id));
                                                cmd.Parameters.Add(new SqlParameter("coupon_id", line_pr_adj_coupon_id));
                                                cmd.Parameters.Add(new SqlParameter("user_id", DBNull.Value));
                                                cmd.ExecuteNonQuery();
                                                // File.AppendAllText(logFile, DateTime.Now.ToString() + " - " +
                                                //        "dw_OrderLineItem_PriceAdj_insertupdate " + Environment.NewLine);
                                                ProcessLogLib.ProcessLog.Insert(ProcessRunID, ProcessLogLib.ProcessLogMessageType.Info, "",
                                                   "dw_OrderLineItem_PriceAdj_insertupdate ");
                                            }
                                        }
                                    } // nodeLinePrAdjs != null
                                } // product_lineitem
                            } // loop on product_lineitems
                        } // nodeordersub name = product_lineitems
                        if (nodeOrderSub.Name == "shipping-lineitems")
                        {
                            string unit_price = "", tax = "", lineitem_text = "", item_id = "",
                                gross_price = "", base_price = "",
                                ship_line_shipment_id = "", tax_rate = "";
                            foreach (XmlNode nodeItems in nodeOrderSub.ChildNodes)
                            {
                                if (nodeItems.Name == "shipping-lineitem")
                                {
                                    XmlNode nodeLinePrAdjs = null;
                                    foreach (XmlNode nodeItem in nodeItems.ChildNodes)
                                    {
                                        if (nodeItem.Name == "net-price")
                                            unit_price = nodeItem.InnerText;
                                        if (nodeItem.Name == "tax")
                                            tax = nodeItem.InnerText;
                                        if (nodeItem.Name == "lineitem-text")
                                            lineitem_text = nodeItem.InnerText;
                                        if (nodeItem.Name == "gross-price")
                                            gross_price = nodeItem.InnerText;
                                        if (nodeItem.Name == "base-price")
                                            base_price = nodeItem.InnerText;
                                        if (nodeItem.Name == "tax-rate")
                                            tax_rate = nodeItem.InnerText;
                                        if (nodeItem.Name == "item-id")
                                            item_id = nodeItem.InnerText;
                                        if (nodeItem.Name == "shipment-id")
                                            ship_line_shipment_id = nodeItem.InnerText;
                                        if (nodeItem.Name == "price-adjustments")
                                        {
                                            nodeLinePrAdjs = nodeItem;
                                        }
                                    }
                                    decimal unitprice = 0;
                                    if (!decimal.TryParse(unit_price, out unitprice)) unitprice = 0;
                                    decimal taxamt = 0;
                                    if (!decimal.TryParse(tax, out taxamt)) taxamt = 0;
                                    decimal grossprice = 0;
                                    if (!decimal.TryParse(gross_price, out grossprice)) grossprice = 0;
                                    decimal taxrate = 0;
                                    if (!decimal.TryParse(tax_rate, out taxrate)) taxrate = 0;
                                    decimal baseprice = 0;
                                    if (!decimal.TryParse(base_price, out baseprice)) baseprice = 0;

                                    // insert into dw_OrderFreight
                                    sql = "dw_OrderFreight_insertupdate";
                                    cmd = new SqlCommand(sql, conn);
                                    cmd.CommandType = CommandType.StoredProcedure;
                                    cmd.Parameters.Add(new SqlParameter("order_no", order_no));
                                    cmd.Parameters.Add(new SqlParameter("net_price", unitprice));
                                    cmd.Parameters.Add(new SqlParameter("tax", taxamt));
                                    cmd.Parameters.Add(new SqlParameter("gross_price", gross_price));
                                    cmd.Parameters.Add(new SqlParameter("base_price", baseprice));
                                    cmd.Parameters.Add(new SqlParameter("line_item", lineitem_text));
                                    cmd.Parameters.Add(new SqlParameter("item_id", item_id));
                                    cmd.Parameters.Add(new SqlParameter("shipment_id", ship_line_shipment_id));
                                    cmd.Parameters.Add(new SqlParameter("tax_rate", taxrate));
                                    cmd.Parameters.Add(new SqlParameter("user_id", DBNull.Value));
                                    cmd.Parameters.Add(new SqlParameter("order_freight_id", SqlDbType.Int, 4, ParameterDirection.InputOutput, false, 0, 0,
                                        "order_freight_id", DataRowVersion.Default, DBNull.Value));
                                    cmd.ExecuteNonQuery();
                                    //File.AppendAllText(logFile, DateTime.Now.ToString() + " - " +
                                    //    "dw_OrderFreight_insertupdate " + Environment.NewLine);
                                    ProcessLogLib.ProcessLog.Insert(ProcessRunID, ProcessLogLib.ProcessLogMessageType.Info, "",
                                      "dw_OrderFreight_insertupdate ");
                                    int orderFreightID = (int)cmd.Parameters["order_freight_id"].Value;
                                    // insert into dw_OrderFreight_PriceAdj
                                    if (nodeLinePrAdjs != null)
                                    {
                                        foreach (XmlNode nodepradj in nodeLinePrAdjs.ChildNodes)
                                        {
                                            if (nodepradj.Name == "price-adjustment")
                                            {
                                                string line_pr_adj_net_price = "", line_pr_adj_tax = "", line_pr_adj_gross_price = "",
                                                    line_pr_adj_base_price = "", line_pr_adj_lineitem_text = "",
                                                    line_pr_adj_promotion_id = "", line_pr_adj_campaign_id = "",
                                                    line_pr_adj_coupon_id = "";
                                                foreach (XmlNode nodepradjsub in nodepradj.ChildNodes)
                                                {
                                                    if (nodepradjsub.Name == "net-price")
                                                        line_pr_adj_net_price = nodepradjsub.InnerText;
                                                    if (nodepradjsub.Name == "tax")
                                                        line_pr_adj_tax = nodepradjsub.InnerText;
                                                    if (nodepradjsub.Name == "gross-price")
                                                        line_pr_adj_gross_price = nodepradjsub.InnerText;
                                                    if (nodepradjsub.Name == "base-price")
                                                        line_pr_adj_base_price = nodepradjsub.InnerText;
                                                    if (nodepradjsub.Name == "lineitem-text")
                                                        line_pr_adj_lineitem_text = nodepradjsub.InnerText;
                                                    if (nodepradjsub.Name == "promotion-id")
                                                        line_pr_adj_promotion_id = nodepradjsub.InnerText;
                                                    if (nodepradjsub.Name == "campaign-id")
                                                        line_pr_adj_campaign_id = nodepradjsub.InnerText;
                                                    if (nodepradjsub.Name == "coupon-id")
                                                        line_pr_adj_coupon_id = nodepradjsub.InnerText;
                                                }
                                                decimal linepradjnetprice = 0;
                                                if (!decimal.TryParse(line_pr_adj_net_price, out linepradjnetprice)) linepradjnetprice = 0;
                                                decimal linepradjgrossprice = 0;
                                                if (!decimal.TryParse(line_pr_adj_gross_price, out linepradjgrossprice)) linepradjgrossprice = 0;
                                                decimal linepradjbaseprice = 0;
                                                if (!decimal.TryParse(line_pr_adj_base_price, out linepradjbaseprice)) linepradjbaseprice = 0;
                                                decimal linepradjtax = 0;
                                                if (!decimal.TryParse(line_pr_adj_tax, out linepradjtax)) linepradjtax = 0;
                                                // insert into dw_OrderLineItem_priceAdj
                                                sql = "dw_OrderFreight_PriceAdj_insertupdate";
                                                cmd = new SqlCommand(sql, conn);
                                                cmd.CommandType = CommandType.StoredProcedure;
                                                cmd.Parameters.Add(new SqlParameter("order_freight_id", orderFreightID));
                                                cmd.Parameters.Add(new SqlParameter("order_no", order_no));
                                                cmd.Parameters.Add(new SqlParameter("net_price", linepradjnetprice));
                                                cmd.Parameters.Add(new SqlParameter("tax", linepradjtax));
                                                cmd.Parameters.Add(new SqlParameter("gross_price", linepradjgrossprice));
                                                cmd.Parameters.Add(new SqlParameter("base_price", linepradjbaseprice));
                                                cmd.Parameters.Add(new SqlParameter("line_item", line_pr_adj_lineitem_text));
                                                cmd.Parameters.Add(new SqlParameter("promotion_id", line_pr_adj_promotion_id));
                                                cmd.Parameters.Add(new SqlParameter("campaign_id", line_pr_adj_campaign_id));
                                                cmd.Parameters.Add(new SqlParameter("coupon_id", line_pr_adj_coupon_id));
                                                cmd.Parameters.Add(new SqlParameter("user_id", DBNull.Value));
                                                cmd.ExecuteNonQuery();
                                                // File.AppendAllText(logFile, DateTime.Now.ToString() + " - " +
                                                //    "dw_OrderFreight_PriceAdj_insertupdate " + Environment.NewLine);
                                                ProcessLogLib.ProcessLog.Insert(ProcessRunID, ProcessLogLib.ProcessLogMessageType.Info, "",
                                                  "dw_OrderFreight_PriceAdj_insertupdate ");
                                            }
                                        }
                                    } // nodeLinePrAdjs != null
                                } // shippinglineitem
                            } // loop shippinglineitems
                        }// nodeOrdeSub name = shipping_lineitems
                    } // order lines foreach NodeOrderSub
                    if (!Recover)
                    {
                        sqlAudit = "update dw_order_audit set completeddate = getdate() where order_audit_id = " + auditID.ToString();
                        cmdAudit = new SqlCommand(sqlAudit, connAudit);
                        cmdAudit.CommandType = CommandType.Text;
                        cmdAudit.ExecuteNonQuery();
                        connAudit.Close();
                    }
                }
                catch (Exception ex)
                {
                    if (ex.Message.ToLower().IndexOf("duplicate") < 0)
                    {
                        Console.Write("Unexpected error found: " + ex.Message + Environment.NewLine +
                            ex.StackTrace);
                        //File.AppendAllText(logFile, DateTime.Now.ToString() + " - " +
                        //"Unexpected error found:  " + ex.Message + Environment.NewLine + ex.StackTrace + Environment.NewLine);
                        File.WriteAllText(monFile, "FAIL");
                        ProcessLogLib.ProcessLog.Insert(ProcessRunID, ProcessLogLib.ProcessLogMessageType.Error, "-1",
                          "Unexpected error found:  " + ex.Message + Environment.NewLine + ex.StackTrace);
                        return;
                    }
                }
            } // main xml loop
        }

        static void Main(string[] args)
        {
            string downloadFolder = "";
            ProcessLogLib.ProcessRun procrun = null;
            try
            {
                procrun = new ProcessLogLib.ProcessRun();
                procrun.ProcessId = "DemandwareDownload";
                procrun.Start();
                AppSettingsReader appSettingsReader = new AppSettingsReader();
                downloadFolder = (String)appSettingsReader.GetValue("DownloadFrom", downloadFolder.GetType());
                string SFTPHost = (String)appSettingsReader.GetValue("SFTPServer", downloadFolder.GetType());
                string SFTPUser = (String)appSettingsReader.GetValue("SFTPUser", downloadFolder.GetType());
                string SFTPPW = (String)appSettingsReader.GetValue("SFTPPW", downloadFolder.GetType());
                string recvFolder = (String)appSettingsReader.GetValue("ReceiveFolder", downloadFolder.GetType());
                //logFile = (String)appSettingsReader.GetValue("LogFile", downloadFolder.GetType());
                monFile = (String)appSettingsReader.GetValue("MonitorFile", downloadFolder.GetType());
                //File.AppendAllText(logFile, DateTime.Now.ToString() + " *********  START DemandWareDownload  *********" + Environment.NewLine);
                File.WriteAllText(monFile, "OK");
                string connString = ConfigurationManager.ConnectionStrings["Demandware"].ConnectionString;
                SqlConnection conn = new SqlConnection(connString);
                conn.Open();
                SqlConnection conn2 = new SqlConnection(connString);
                conn2.Open();
                if (args.Length > 0)
                {
                    string fileContent = System.IO.File.ReadAllText(args[0]);
                    //File.AppendAllText(logFile, DateTime.Now.ToString() + " - " +
                    //    "New file to be processed: " + args[0] + Environment.NewLine);
                    ProcessLogLib.ProcessLog.Insert(procrun.ProcessRunId, ProcessLogLib.ProcessLogMessageType.Info, "",
                      "New file to be processed: " + args[0]);
                    LoadOrders(conn, args[0], fileContent, false, procrun.ProcessRunId);
                }
                else
                {
                    // re process files that were interrupted
                    string connStringAudit = ConfigurationManager.ConnectionStrings["AuditLog"].ConnectionString;
                    SqlConnection connAudit = new SqlConnection(connStringAudit);
                    connAudit.Open();
                    string sql = "dw_OrdersNotImported";
                    SqlCommand cmd = new SqlCommand(sql, connAudit);
                    cmd.CommandType = CommandType.StoredProcedure;
                    using (SqlDataReader rdr = cmd.ExecuteReader())
                    {
                        while (rdr.Read())
                        {
                            string fn = rdr["filename"].ToString();
                            //File.AppendAllText(logFile, DateTime.Now.ToString() + " - " +
                            //        "Interrupted file to be processed: " + fn + ", order: " + rdr["order_no"].ToString() +
                            //        Environment.NewLine);
                            ProcessLogLib.ProcessLog.Insert(procrun.ProcessRunId, ProcessLogLib.ProcessLogMessageType.Info, "",
                              "Interrupted file to be processed: " + fn + ", order: " + rdr["order_no"].ToString());
                            string fileContent = "";
                            try
                            {
                                if (fn.IndexOf('\\') > -1 || fn.IndexOf(':') > -1)
                                    fileContent = System.IO.File.ReadAllText(fn);
                                else
                                    fileContent = System.IO.File.ReadAllText(recvFolder + @"\" + fn);
                            }
                            catch (Exception ex)
                            {
                                ProcessLogLib.ProcessLog.Insert(procrun.ProcessRunId, ProcessLogLib.ProcessLogMessageType.Info, "",
                                  "Error with file: " + fn + ex.Message);
                                fileContent = "";
                            }
                            if (fileContent.Length > 0)
                            {
                                // clean up incomplete order
                                string sqlDelOrd = "tdg_delete_DW_order";
                                SqlCommand cmdDelOrd = new SqlCommand(sqlDelOrd, conn2);
                                cmdDelOrd.CommandType = CommandType.StoredProcedure;
                                cmdDelOrd.Parameters.Add(new SqlParameter("DW_OrderNo", rdr["order_no"].ToString()));
                                cmdDelOrd.ExecuteNonQuery();
                                LoadOrders(conn, fn, fileContent, true, procrun.ProcessRunId);
                            }
                        }
                    }
                    connAudit.Close();
                    conn2.Close();
                    // download from SFTP site
                    Sftp sshCp;
                    sshCp = new Sftp(SFTPHost, SFTPUser);
                    sshCp.Password = SFTPPW;
                    sshCp.Connect();
                    ArrayList aryFiles = sshCp.GetFileList(@"/var/www/demandware/" + downloadFolder);
                    //sshCp.Close();
                    foreach (string dwFile in aryFiles)
                    {
                        // what type?
                        if (dwFile.IndexOf("Orders_") > -1 || dwFile.IndexOf("OrdersCS_") > -1)
                        {
                            // download if new
                            //File.AppendAllText(logFile, DateTime.Now.ToString() + " - " +
                            //    "file from Demandware: " + dwFile + Environment.NewLine);
                            if (File.Exists(recvFolder + @"\" + dwFile))
                            {
                                DateTime createDt = File.GetCreationTime(recvFolder + @"\" + dwFile).AddDays(30);
                                if (createDt < DateTime.Today)
                                {
                                    // purge on Demandware server if older than 30 days and move to archive folder
                                    sshCp.Delete(@"/var/www/demandware/" + downloadFolder + @"/" + dwFile);
                                    if (!File.Exists(recvFolder + @"\Archived\" + dwFile))
                                        File.Move(recvFolder + @"\" + dwFile, recvFolder + @"\Archived\" + dwFile);
                                    //File.AppendAllText(logFile, DateTime.Now.ToString() + " - " +
                                    //    "File archived (older than 30 days): " + dwFile + Environment.NewLine);
                                    ProcessLogLib.ProcessLog.Insert(procrun.ProcessRunId, ProcessLogLib.ProcessLogMessageType.Info, "",
                                      "File archived (older than 30 days): " + dwFile);
                                }
                            }
                            else
                            {
                                sshCp.Get(downloadFolder + @"/" + dwFile, recvFolder + @"\" + dwFile);
                                string fileContent = System.IO.File.ReadAllText(recvFolder + @"\" + dwFile);
                                //File.AppendAllText(logFile, DateTime.Now.ToString() + " - " +
                                //    "New file to be processed: " + dwFile + Environment.NewLine);
                                ProcessLogLib.ProcessLog.Insert(procrun.ProcessRunId, ProcessLogLib.ProcessLogMessageType.Info, "",
                                  "New file to be processed: " + dwFile);
                                LoadOrders(conn, dwFile, fileContent, false, procrun.ProcessRunId);
                            }
                        }
                    }
                    sshCp.Close();
                }
                //File.AppendAllText(logFile, DateTime.Now.ToString() + " - " +
                //    "********  End DemandwareDownload ********" + Environment.NewLine);
                if (downloadFolder.ToLower().IndexOf("staging") > -1)
                {
                    Console.Write("press any key...");
                    Console.Read();
                }
            }
            catch (Exception ex)
            {
                Console.Write("Error:" + ex.Message + Environment.NewLine + ex.StackTrace);
                //File.AppendAllText(logFile, DateTime.Now.ToString() + " - " +
                //    "Unexpected error : " + ex.Message + Environment.NewLine + ex.StackTrace + Environment.NewLine);
                File.WriteAllText(monFile, "FAIL");
                ProcessLogLib.ProcessLog.Insert(procrun.ProcessRunId, ProcessLogLib.ProcessLogMessageType.Error, "-1",
                  "Unexpected error : " + ex.Message + Environment.NewLine + ex.StackTrace);
                if (downloadFolder.ToLower().IndexOf("staging") > -1)
                {
                    Console.Write("press any key...");
                    Console.Read();
                }
            }
            finally
            {
                procrun.End();
            }
        }
    }
}
