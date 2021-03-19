using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Client;
using Newtonsoft.Json;
using PortoSeguro.GestaoLeads.Principal.Application.Plugin;
using PortoSeguro.GestaoLeads.Principal.Domain;
using PortoSeguro.GestaoLeads.Principal.Domain.Model;
using PortoSeguro.GestaoLeads.Principal.Domain.Service;
using PortoSeguro.WebJob.HierarquiaComercial;

namespace PortoSeguro.Hierarquia.ProcessarHierarquiaComercial
{
    // To learn more about Microsoft Azure WebJobs SDK, please see https://go.microsoft.com/fwlink/?LinkID=320976
    class Program
    {
        static string _sbConnectionString;
        static string _sbQueueName;
        static string _sbQueue;
        static string _orgDynamics;
        static OrganizationServiceProxy _provider;

        static IQueueClient queueClient;

        static string orgDynamics;

        // Please set the following connection strings in app.config for this WebJob to run:
        // AzureWebJobsDashboard and AzureWebJobsStorage

        static void Main()
        {
            #region Teste Qualificação de Leads
            ////Teste Qualificação de Leads -- 
            ////-----------------------------------------------------------------------
            ////-----------------------------------------------------------------------

            var appSettings = System.Configuration.ConfigurationManager.AppSettings;
            _orgDynamics = appSettings["orgDynamics"] ?? string.Empty;
            string user = appSettings["userDynamics"] ?? string.Empty;
            string pass = appSettings["passDynamics"] ?? string.Empty;
            string domain = appSettings["domainDynamics"] ?? string.Empty;
            string discovery = appSettings["discoveryDynamics"] ?? string.Empty;

            UtilCriarConexao utilCriarConexao = new UtilCriarConexao(_orgDynamics, user, pass, domain, discovery);
            _provider = utilCriarConexao.GerarOrganization();

            Guid familiaProduto = new Guid("0938b48a-8e01-ea11-a811-000d3ac17526");
            Guid? produto = new Guid("09953911-9001-ea11-a811-000d3ac17db3");
            Guid? tipoProduto = null; //new Guid("25420eff-b14e-e911-a95b-000d3ac03cbc");

            Guid? origem = new Guid("25420eff-b14e-e911-a95b-000d3ac03cbc");
            Guid? sucursal = null;

            QualificacaoLeads qualificacaoLeads = new QualificacaoLeads(_orgDynamics, false, _provider);

            List<QualificacaoLeads> listaQualificacaoLeads = qualificacaoLeads.ListarPor(familiaProduto, produto, tipoProduto, origem, sucursal).ToList();


            ScoreSerasaService scoreSerasaService = new ScoreSerasaService(_orgDynamics);
            var retritivos = scoreSerasaService.GetDadosRestritivos("00000012106");
            var score = scoreSerasaService.GetDadosScoreSerasa("00000012106");


            bool scorePossuiRestricao = false;
            bool encerrarPorRestricao = false;

            if (score.OcorreuErro)
            {
                var service = _provider as IOrganizationService;

                Entity leadAtualizar = new Entity("contact");
                //leadAtualizar.Id = lead.Id.Value;
                leadAtualizar["pmc_score_csba"] = null;
                service.Update(leadAtualizar);
            }
            else
            {
                int scoreValue = 0;
                if (score.Scores.CSBA.Contains("."))
                    scoreValue = Convert.ToInt32(score.Scores.CSBA.Split('.')[0]);
                else if (score.Scores.CSBA.Contains(","))
                    scoreValue = Convert.ToInt32(score.Scores.CSBA.Split(',')[0]);
                else
                    scoreValue = Convert.ToInt32(score.Scores.CSBA.Trim());

                var ScoreCSBA = scoreValue;

                Configuracao configuracaoLimiteScoreCSBA = new Configuracao(_orgDynamics, false, _provider)
                    .ObterPor("LimiteScoreCSBA");

                if (configuracaoLimiteScoreCSBA == null)
                {
                    throw new Exception("Configuração 'LimiteScoreCSBA' não encontrada.");
                }

                if (scoreValue < Convert.ToInt32(configuracaoLimiteScoreCSBA.Valor))
                {
                    encerrarPorRestricao = true;

                    #region Definir a Fila Restrição
                    Configuracao configuracaoFilaRestricao = CacheManager.GetCache<Configuracao>(_orgDynamics, "FilaRestricao", null);
                    if (configuracaoFilaRestricao == null)
                        configuracaoFilaRestricao = CacheManager.GetCache<Configuracao>(_orgDynamics, "FilaRestricao", true, null);

                    if (configuracaoFilaRestricao == null) { throw new Exception("Configuração 'FilaRestricao' não encontrada."); }

                    string nomeFila = configuracaoFilaRestricao.Valor;
                    List<Fila> filaRestricaoCache = CacheManager.GetCache<List<Fila>>(_orgDynamics);

                    var resultFilaRestricaoCache = (filaRestricaoCache != null && filaRestricaoCache.Count > 0) ? filaRestricaoCache.FirstOrDefault(fd => fd.Nome.ToLower() == nomeFila.ToLower()) : null;

                    if (resultFilaRestricaoCache != null && resultFilaRestricaoCache.Id != Guid.Empty)
                        ;
                    //this.FilaId = resultFilaRestricaoCache.ToLookup();
                    #endregion
                }
            }

        }

        #endregion

        #region Integração Dados Brutos Siebel
        ////-----------------------------------------------------------------------
        ////-----------------------------------------------------------------------
        //Console.WriteLine("======================================================");
        //Console.WriteLine($"Aguarde Final do processamento. Processo iniciado: {DateTime.Now}");
        //Console.WriteLine("======================================================");

        ////LerSiebelSucursalCSV();
        ////LerSiebelCargoCSV();
        ////LerSiebelMatriculaCSV();
        ////LerSiebelHierarquiaSusepCSV();
        //LerSiebelSusepCSV();

        ////LerSiebelContatoSusepCSV();
        ////LerSiebelDadoBancarioCSV();

        //Console.ReadLine();
        ////-----------------------------------------------------------------------
        ////-----------------------------------------------------------------------
        #endregion

        #region Testes
        ////var teste = GeradorAuthorization.Gerar();

        ////var config = new JobHostConfiguration();

        ////if (config.IsDevelopment)
        ////{
        ////    config.UseDevelopmentSettings();
        ////}

        ////var host = new JobHost(config);
        ////// The following code ensures that the WebJob will be running continuously
        //////host.RunAndBlock();

        //////string autorizacao = GeradorAuthorization.Gerar();
        //////return;

        //Console.WriteLine("Iniciou: " + DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss"));

        ////LerCargosCSV();
        ////LerConsolidadoCSV();
        ////LerHoldingCSV();
        ////LerGrupoCSV();
        ////LerSusepPrincipalCSV();
        ////LerSusepProducaoCSV();

        //Console.WriteLine("======================================================");
        //Console.WriteLine("Aguarde Final do processamento.");
        //Console.WriteLine("======================================================");
        //Console.ReadKey();
        #endregion

        #region Converte CSV pata Json

        static void LerCargosCSV()
        {
            try
            {
                _sbConnectionString = "Endpoint=sb://servicebusporto.servicebus.windows.net/;SharedAccessKeyName=Admin;SharedAccessKey=z8tqV9UnwveAWs+FfhnaYMiUqmzEelCwtcDc6tsZobw=";
                _sbQueueName = "portoseguro.hierarquiacomercial.cargo";
                queueClient = new QueueClient(_sbConnectionString, _sbQueueName);

                string sfileName = @"C:\Users\jdias\Desktop\Nova pasta\Hierarquia_v3\1-Cargos.csv";
                List<string> posicaoNome = new List<string>();
                posicaoNome.Add("NivelCargo");
                posicaoNome.Add("CodigoCargo");
                posicaoNome.Add("NomeCargo");
                posicaoNome.Add("CodigoCargoPai");
                posicaoNome.Add("TipoCRUD");

                List<string> result = ConverterCSVparaJson(sfileName, posicaoNome);
                if (result.Count > 0)
                {
                    var task = Enviar(result);
                }

                System.Console.WriteLine($"\n\n\t\t Quantidade de registros: {result.Count.ToString()}");
            }
            catch (Exception ex)
            {
                System.Console.WriteLine($"\t\t Erro Geral: {ex.Message}");
            }
        }

        static void LerConsolidadoCSV()
        {
            try
            {
                _sbConnectionString = "Endpoint=sb://servicebusporto.servicebus.windows.net/;SharedAccessKeyName=Admin;SharedAccessKey=HGfW2mfdj3pTYJdjh2jFmItT6TmxKfU9GM7sJKGfZJE=";
                _sbQueueName = "portoseguro.hierarquiacorretora.consolidado";
                queueClient = new QueueClient(_sbConnectionString, _sbQueueName);

                string sfileName = @"C:\Users\jdias\Desktop\Nova pasta\Hierarquia_v3\2-Consolidados.csv";
                List<string> posicaoNome = new List<string>();
                posicaoNome.Add("codigo");
                posicaoNome.Add("nome");
                posicaoNome.Add("tipo_consolidado");

                List<string> result = ConverterCSVparaJson(sfileName, posicaoNome);
                if (result.Count > 0)
                {
                    var task = Enviar(result);
                }

                System.Console.WriteLine($"\n\n\t\t Quantidade de registros: {result.Count.ToString()}");
            }
            catch (Exception ex)
            {
                System.Console.WriteLine($"\t\t Erro Geral: {ex.Message}");
            }
        }

        static void LerHoldingCSV()
        {
            try
            {
                _sbConnectionString = "Endpoint=sb://servicebusporto.servicebus.windows.net/;SharedAccessKeyName=Admin;SharedAccessKey=OVrAqSN/57zko4WkZI2qF57SwVbfEvdoGQs4+xiqLjY=";
                _sbQueueName = "portoseguro.hierarquiacorretora.holding";
                queueClient = new QueueClient(_sbConnectionString, _sbQueueName);

                string sfileName = @"C:\Users\jdias\Desktop\Nova pasta\Hierarquia_v3\3-Holdings.csv";
                List<string> posicaoNome = new List<string>();
                posicaoNome.Add("consolidado");
                posicaoNome.Add("codigo");
                posicaoNome.Add("nome");
                posicaoNome.Add("tipo_holding");
                posicaoNome.Add("tipo_consolidado");

                List<string> result = ConverterCSVparaJson(sfileName, posicaoNome);
                if (result.Count > 0)
                {
                    var task = Enviar(result);
                }

                System.Console.WriteLine($"\n\n\t\t Quantidade de registros: {result.Count.ToString()}");
            }
            catch (Exception ex)
            {
                System.Console.WriteLine($"\t\t Erro Geral: {ex.Message}");
            }
        }

        static void LerGrupoCSV()
        {
            try
            {
                _sbConnectionString = "Endpoint=sb://servicebusporto.servicebus.windows.net/;SharedAccessKeyName=Admin;SharedAccessKey=waI242gZZjy6LrtEigQY3SqHwXPf1/cHR9k5l8wHYvM=";
                _sbQueueName = "portoseguro.hierarquiacorretora.grupo";
                queueClient = new QueueClient(_sbConnectionString, _sbQueueName);

                string sfileName = @"C:\Users\bfarber\Desktop\Hierarquia_v3\4-Grupos-01.csv";
                List<string> posicaoNome = new List<string>();
                posicaoNome.Add("consolidado");
                posicaoNome.Add("holding");
                posicaoNome.Add("codigo");
                posicaoNome.Add("nome");
                posicaoNome.Add("tipo_grupo");
                posicaoNome.Add("tipo_consolidado");
                posicaoNome.Add("tipo_holding");

                List<string> result = ConverterCSVparaJson(sfileName, posicaoNome);
                if (result.Count > 0)
                {
                    var task = Enviar(result);
                }

                System.Console.WriteLine($"\n\n\t\t Quantidade de registros: {result.Count.ToString()}");
            }
            catch (Exception ex)
            {
                System.Console.WriteLine($"\t\t Erro Geral: {ex.Message}");
            }
        }

        static void LerSusepPrincipalCSV()
        {
            try
            {
                _sbConnectionString = "Endpoint=sb://servicebusporto.servicebus.windows.net/;SharedAccessKeyName=Admin;SharedAccessKey=EcYbUoNhfUHvaVQpSHXVSbXzJ5TDM0sHKbCjG/hsGcQ=";
                _sbQueueName = "portoseguro.hierarquiacorretora.susep";
                queueClient = new QueueClient(_sbConnectionString, _sbQueueName);

                string sfileName = @"C:\Users\wille\Downloads\Suseps_Teste_Log_Integracao.csv"; // C:\Users\bfarber\Desktop\Hierarquia_v3\5-SusepPrincipal-08.csv";
                List<string> posicaoNome = new List<string>();
                posicaoNome.Add("diretor");
                posicaoNome.Add("gerente_sucursal");
                posicaoNome.Add("Sucursal");
                posicaoNome.Add("gerente_regional");
                posicaoNome.Add("gerente_comercial");
                posicaoNome.Add("CodigoConsolidado");
                posicaoNome.Add("CodigoHolding");
                posicaoNome.Add("tipo_grupo");
                posicaoNome.Add("tipo_consolido");
                posicaoNome.Add("tipo_holding");
                posicaoNome.Add("CodigoGrupo");
                posicaoNome.Add("Codigo");
                posicaoNome.Add("RazaoSocial");
                posicaoNome.Add("status");
                posicaoNome.Add("tipo_pessoa");
                posicaoNome.Add("dddComercial");
                posicaoNome.Add("numTelComercial");
                posicaoNome.Add("dddComTelefone");
                posicaoNome.Add("Email");
                posicaoNome.Add("endereco");
                posicaoNome.Add("complemento");
                posicaoNome.Add("bairro");
                posicaoNome.Add("cidade");
                posicaoNome.Add("cep");
                posicaoNome.Add("estado");
                posicaoNome.Add("CPF");
                posicaoNome.Add("CNPJ");
                posicaoNome.Add("tipo_susep");
                posicaoNome.Add("IndicadorSusepPrincipal");
                posicaoNome.Add("nivel_gerente_regional");
                posicaoNome.Add("nivel_gerente_sucursal");
                posicaoNome.Add("nivel_diretor");
                posicaoNome.Add("status_multicanal");
                posicaoNome.Add("DataEfetivacao");
                posicaoNome.Add("DataStatus");
                posicaoNome.Add("celular");

                List<string> result = ConverterCSVparaJson(sfileName, posicaoNome);
                if (result.Count > 0)
                {
                    var task = Enviar(result);
                }

                System.Console.WriteLine($"\n\n\t\t Quantidade de registros: {result.Count.ToString()}");
            }
            catch (Exception ex)
            {
                System.Console.WriteLine($"\t\t Erro Geral: {ex.Message}");
            }
        }

        static void LerSusepProducaoCSV()
        {
            try
            {
                _sbConnectionString = "Endpoint=sb://servicebusporto.servicebus.windows.net/;SharedAccessKeyName=Admin;SharedAccessKey=EcYbUoNhfUHvaVQpSHXVSbXzJ5TDM0sHKbCjG/hsGcQ=";
                _sbQueueName = "portoseguro.hierarquiacorretora.susep";
                queueClient = new QueueClient(_sbConnectionString, _sbQueueName);

                string sfileName = @"C:\Users\bfarber\Desktop\Hierarquia_v3\6-SusepProducao.csv";
                List<string> posicaoNome = new List<string>();
                posicaoNome.Add("diretor");
                posicaoNome.Add("gerente_sucursal");
                posicaoNome.Add("Sucursal");
                posicaoNome.Add("gerente_regional");
                posicaoNome.Add("gerente_comercial");
                posicaoNome.Add("CodigoConsolidado");
                posicaoNome.Add("CodigoHolding");
                posicaoNome.Add("tipo_grupo");
                posicaoNome.Add("tipo_consolido");
                posicaoNome.Add("tipo_holding");
                posicaoNome.Add("CodigoGrupo");
                posicaoNome.Add("CodigoSusepPrincipal");
                posicaoNome.Add("Codigo");
                posicaoNome.Add("RazaoSocial");
                posicaoNome.Add("status");
                posicaoNome.Add("tipo_pessoa");
                posicaoNome.Add("dddComercial");
                posicaoNome.Add("numTelComercial");
                posicaoNome.Add("dddComTelefone");
                posicaoNome.Add("Email");
                posicaoNome.Add("endereco");
                posicaoNome.Add("complemento");
                posicaoNome.Add("bairro");
                posicaoNome.Add("cidade");
                posicaoNome.Add("cep");
                posicaoNome.Add("estado");
                posicaoNome.Add("CPF");
                posicaoNome.Add("CNPJ");
                posicaoNome.Add("tipo_susep");
                posicaoNome.Add("IndicadorSusepPrincipal");
                posicaoNome.Add("nivel_gerente_regional");
                posicaoNome.Add("nivel_gerente_sucursal");
                posicaoNome.Add("nivel_diretor");
                posicaoNome.Add("status_multicanal");
                posicaoNome.Add("DataEfetivacao");
                posicaoNome.Add("DataStatus");
                posicaoNome.Add("celular");

                List<string> result = ConverterCSVparaJson(sfileName, posicaoNome);
                if (result.Count > 0)
                {
                    var task = Enviar(result);
                }

                System.Console.WriteLine($"\n\n\t\t Quantidade de registros: {result.Count.ToString()}");
            }
            catch (Exception ex)
            {
                System.Console.WriteLine($"\t\t Erro Geral: {ex.Message}");
            }
        }

        static List<string> ConverterCSVparaJson(string sfileName, List<string> posicaoNome)
        {
            List<string> resultJson = new List<string>();

            //string sfileName = @"C:\Users\jdias\Desktop\Carga de Dados 21-03\1-Cargos.csv";
            using (StreamReader sr = new StreamReader(sfileName, Encoding.GetEncoding("ISO-8859-1")))
            {
                sr.ReadLine();
                do
                {
                    string s = sr.ReadLine();
                    string[] arrData = s.Split(';');
                    string resultLinha = "{ ";
                    string virgula = "";

                    //Valido apenas para Cargo:
                    if (posicaoNome.Contains("NivelCargo"))
                    {
                        if (arrData[1].ToLower() == "vice-presidente" || arrData[1].ToLower() == "vp")
                            arrData[6] = "0";
                    }

                    int qtdCampoVazio = 0;
                    for (int i = 0; i < posicaoNome.Count(); i++)
                    {
                        if (arrData[i] == "")
                            qtdCampoVazio++;

                        resultLinha = string.Format("{0} {1} \"{2}\":\"{3}\"", resultLinha, virgula, posicaoNome[i], arrData[i]);
                        virgula = ", ";
                    }

                    if (qtdCampoVazio < posicaoNome.Count())
                        resultJson.Add(resultLinha + " }");

                } while (!(sr.EndOfStream));
            }

            return resultJson;
        }

        //CodePage - 850
        static string ConverterStringAcentuada(int codepage, string texto)
        {
            return Encoding.ASCII.GetString(Encoding.GetEncoding(codepage).GetBytes(texto));
        }

        public static async Task Enviar(List<string> msgs)
        {
            queueClient = new QueueClient(_sbConnectionString, _sbQueueName);

            Console.WriteLine("======================================================");
            Console.WriteLine("Enviando ao Service Bus: " + DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss"));
            Console.WriteLine("======================================================");

            // Send messages.
            await SendMessagesAsync(msgs);

            Console.WriteLine("Terminou Envio: " + DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss"));

            await queueClient.CloseAsync();

            Console.WriteLine("Terminou conexão: " + DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss"));
            Console.WriteLine("Finalizado, aperte qualquer tecla para sair.");
        }

        static async Task SendMessagesAsync(List<string> msgs)
        {
            try
            {
                foreach (var item in msgs)
                {
                    var message = new Message(Encoding.UTF8.GetBytes(item));
                    await queueClient.SendAsync(message);
                }
            }
            catch (Exception exception)
            {
                Console.WriteLine($"{DateTime.Now} :: Exception: {exception.Message}");
            }
        }

        #endregion


        /////////////////////// SUSEP DADOS BRUTOS ///////////////////////

        #region Converte CSV Siebel para Json e evia para API

        static void LerSiebelSucursalCSV()
        {
            try
            {
                string sfileName = @"D:\DadosBrutosSiebel-Teste\Export_SUCURSAL_02062020.csv";
                List<string> posicaoNome = new List<string>();
                posicaoNome.Add("CodigoSucursal");
                posicaoNome.Add("NomeSucursal");
                posicaoNome.Add("UF");
                posicaoNome.Add("Status");
                posicaoNome.Add("TipoCRUD");

                List<string> result = ConverterCSVparaJson(sfileName, posicaoNome);
                if (result.Count > 0)
                {
                    EnviarSiebel(result, "sucursais/service-bus");
                }

                Console.WriteLine($"\n\n\t\t Quantidade de registros: {result.Count.ToString()}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\t\t Erro Geral: {ex.Message}");
            }
        }

        static void LerSiebelCargoCSV()
        {
            try
            {
                string sfileName = @"D:\DadosBrutosSiebel-Teste\Export_CARGO_03062020.csv";
                List<string> posicaoNome = new List<string>();
                posicaoNome.Add("RowIdCargo");
                posicaoNome.Add("NivelCargo");
                posicaoNome.Add("CodigoCargo");
                posicaoNome.Add("NomeCargo");
                posicaoNome.Add("Status");
                posicaoNome.Add("RowIdCargoPai");
                posicaoNome.Add("CodigoCargoPai");
                posicaoNome.Add("TipoCRUD");

                List<string> result = ConverterCSVparaJson(sfileName, posicaoNome);
                if (result.Count > 0)
                {
                    EnviarSiebel(result, "cargos/service-bus");
                }

                Console.WriteLine($"\n\n\t\t Quantidade de registros: {result.Count.ToString()}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\t\t Erro Geral: {ex.Message}");
            }
        }

        static void LerSiebelDadoBancarioCSV()
        {
            try
            {
                string sfileName = @"D:\DadosSiebel\01-Export_DadosBancarios_SUSEP_1205.csv";
                List<string> posicaoNome = new List<string>();
                posicaoNome.Add("NumeroBanco");
                posicaoNome.Add("Banco");
                posicaoNome.Add("Agencia");
                posicaoNome.Add("Conta");
                posicaoNome.Add("Digito");
                posicaoNome.Add("SusepConta");
                posicaoNome.Add("TipoCRUD");

                List<string> result = ConverterCSVparaJson(sfileName, posicaoNome);
                if (result.Count > 0)
                {
                    EnviarSiebel(result, "dadosbancarios/service-bus");
                }

                Console.WriteLine($"\n\n\t\t Quantidade de registros: {result.Count.ToString()}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\t\t Erro Geral: {ex.Message}");
            }
        }

        static void LerSiebelHierarquiaSusepCSV()
        {
            try
            {
                string sfileName = @"D:\DadosBrutosSiebel-Teste\Export_HIERARQUIA_02062020.csv";
                List<string> posicaoNome = new List<string>();
                posicaoNome.Add("TipoHierarquia");
                posicaoNome.Add("CodigoHierarquia");
                posicaoNome.Add("NomeHierarquia");
                posicaoNome.Add("CodigoHierarquiaSuperior");
                posicaoNome.Add("DataInclusao");
                posicaoNome.Add("Status");
                posicaoNome.Add("TipoCRUD");

                List<string> result = ConverterCSVparaJson(sfileName, posicaoNome);
                if (result.Count > 0)
                {
                    EnviarSiebel(result, "hierarquiassuseps/service-bus");
                }

                Console.WriteLine($"\n\n\t\t Quantidade de registros: {result.Count.ToString()}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\t\t Erro Geral: {ex.Message}");
            }
        }

        static void LerSiebelMatriculaCSV()
        {
            try
            {
                string sfileName = @"D:\DadosBrutosSiebel-Teste\Export_Matriculas_1205.csv";
                List<string> posicaoNome = new List<string>();
                posicaoNome.Add("Cargo");
                posicaoNome.Add("Matricula");
                posicaoNome.Add("NivelCargo");
                posicaoNome.Add("Email");
                posicaoNome.Add("Status");
                posicaoNome.Add("TipoCRUD");

                List<string> result = ConverterCSVparaJson(sfileName, posicaoNome);
                if (result.Count > 0)
                {
                    EnviarSiebel(result, "matriculas/service-bus");
                }

                Console.WriteLine($"\n\n\t\t Quantidade de registros: {result.Count.ToString()}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\t\t Erro Geral: {ex.Message}");
            }
        }

        static void LerSiebelSusepCSV()
        {
            try
            {
                string sfileName = @"D:\DadosBrutosSiebel-Teste\Export_SUSEP_PAI_ATIVA_03062020-TESTE-2.csv";
                List<string> posicaoNome = new List<string>();
                posicaoNome.Add("CodigoVP");
                posicaoNome.Add("CodigoDG");
                posicaoNome.Add("CodigoDC");
                posicaoNome.Add("CodigoGS");
                posicaoNome.Add("CodigoSucursal");
                posicaoNome.Add("CodigoGR");
                posicaoNome.Add("CodigoGC");
                posicaoNome.Add("CodigoConsolidado");
                posicaoNome.Add("CodigoHolding");
                posicaoNome.Add("CodigoGrupo");
                posicaoNome.Add("CodigoSusepPrincipal");
                posicaoNome.Add("SusepPorto");
                posicaoNome.Add("Corretora");
                posicaoNome.Add("Status");
                posicaoNome.Add("TipoPessoa");
                posicaoNome.Add("Email");
                posicaoNome.Add("Endereco");
                posicaoNome.Add("Complemento");
                posicaoNome.Add("Bairro");
                posicaoNome.Add("Cidade");
                posicaoNome.Add("Cep");
                posicaoNome.Add("Estado");
                posicaoNome.Add("Cpf");
                posicaoNome.Add("PesCod");
                posicaoNome.Add("Cnpj");
                posicaoNome.Add("TipoConta");
                posicaoNome.Add("SusepPrincipal");
                posicaoNome.Add("StatusMulticanal");
                posicaoNome.Add("DataEfetivacao");
                posicaoNome.Add("DataStatus");
                posicaoNome.Add("BloqueioProducao");
                posicaoNome.Add("BloqueioFenacor");
                posicaoNome.Add("BloqueioComissoes");
                posicaoNome.Add("BloqueioInativos");
                posicaoNome.Add("BloqueioJustificativa");
                posicaoNome.Add("TipoCRUD");

                List<string> result = ConverterCSVparaJson(sfileName, posicaoNome);

                //Gerar jSon de telefones de susep
                string sfileNameTelefones = @"D:\DadosSiebel\Export_Telefone_SUSEP_ATIVO_1405.csv";
                List<string> posicaoNomeTelefone = new List<string>();
                posicaoNomeTelefone.Add("IdVinculo");
                posicaoNomeTelefone.Add("RowId");
                posicaoNomeTelefone.Add("DDDTelefone");
                posicaoNomeTelefone.Add("TipoTelefone");
                posicaoNomeTelefone.Add("TelefonePrincipal");
                posicaoNomeTelefone.Add("TipoCRUD");

                List<string> telefones = ConverterCSVparaJson(sfileNameTelefones, posicaoNomeTelefone);

                if (result.Count > 0)
                {
                    EnviarSiebelSusep(result, telefones, "suseps/service-bus");
                }

                Console.WriteLine($"\n\n\t\t Quantidade de registros: {result.Count.ToString()}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\t\t Erro Geral: {ex.Message}");
            }
        }

        static void LerSiebelContatoSusepCSV()
        {
            try
            {
                //Gerar jSon de contatos
                string sfileContatos = @"D:\DadosBrutosSiebel-Teste\Export_CONTATO_SUSEP_PAI_ATIVA_09062020-TESTE-2.csv";
                List<string> posicaoNomeContato = new List<string>();
                posicaoNomeContato.Add("SusepPorto");
                posicaoNomeContato.Add("RowId");
                posicaoNomeContato.Add("RowIdSusep"); //........Não usamos essa informção
                posicaoNomeContato.Add("Cpf");
                posicaoNomeContato.Add("Tipo");
                posicaoNomeContato.Add("SubTipo");
                posicaoNomeContato.Add("Nome");
                posicaoNomeContato.Add("Sobrenome");
                posicaoNomeContato.Add("PesCod");
                posicaoNomeContato.Add("DataNascimento");

                posicaoNomeContato.Add("Telefone"); //...........Não usamos essa informção
                posicaoNomeContato.Add("TipoTelefone"); //.......Não usamos essa informção
                posicaoNomeContato.Add("TelefonePrincipal"); //..Não usamos essa informção

                posicaoNomeContato.Add("Email");
                posicaoNomeContato.Add("Endereco");
                posicaoNomeContato.Add("Complemento");
                posicaoNomeContato.Add("Bairro");
                posicaoNomeContato.Add("Cidade");
                posicaoNomeContato.Add("Cep");
                posicaoNomeContato.Add("Estado");
                posicaoNomeContato.Add("StatusRadar");
                posicaoNomeContato.Add("TipoCRUD");
                List<string> result = ConverterCSVparaJson(sfileContatos, posicaoNomeContato);

                //Gerar jSon de telefones de contatos
                string sfileNameTelefones = @"D:\DadosSiebel\Export_TelefoneContato_SUSEP_ATIVO_1405.csv";
                List<string> posicaoNomeTelefone = new List<string>();
                posicaoNomeTelefone.Add("IdVinculo");
                posicaoNomeTelefone.Add("RowId");
                posicaoNomeTelefone.Add("DDDTelefone");
                posicaoNomeTelefone.Add("TipoTelefone");
                posicaoNomeTelefone.Add("TelefonePrincipal");
                posicaoNomeTelefone.Add("TipoCRUD");
                List<string> telefones = ConverterCSVparaJson(sfileNameTelefones, posicaoNomeTelefone);

                if (result.Count > 0)
                {
                    EnviarSiebelContatos(result, telefones, "contatossuseps/service-bus");
                }

                Console.WriteLine($"\n\n\t\t Quantidade de registros: {result.Count.ToString()}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\t\t Erro Geral: {ex.Message}");
            }
        }

        public static async void EnviarSiebel(List<string> result, string endPoint)
        {
            try
            {
                int contador = 0;
                int totalLinhas = result.Count;
                int sucesso = 0;
                int failed = 0;
                var inicio = DateTime.Now;
                string nomeOrigem_old = "";

                Dictionary<int, string> logErro = new Dictionary<int, string>();
                IList<Message> messages = new List<Message>();
                ServiceBusRetorno response = new ServiceBusRetorno();

                HttpClientIntegracaoSiebel httpClient = new HttpClientIntegracaoSiebel();
                foreach (var item in result)
                {
                    contador++;
                    if (contador > 0)
                    {
                        Message message = null;
                        response = await httpClient.ApiIntegracaoSiebel(item, endPoint);

                        if (contador == 1)
                            nomeOrigem_old = response.Origem;

                        if (response.Status.ToString() == "200")
                        {
                            sucesso++;
                            if (nomeOrigem_old.ToLower() != response.Origem.ToLower())
                            {
                                string queueName = "ServiceBusQueue" + nomeOrigem_old;
                                string connName = "ServiceBusConnectionString" + nomeOrigem_old;
                                await GravarServiceBusList(connName, queueName, messages);
                                messages = new List<Message>();
                            }

                            Object mensagemObj = response.Mensagem;
                            message = new Message(Encoding.UTF8.GetBytes(mensagemObj.ToString()));
                            messages.Add(message);
                            if (messages.Count >= 200)
                            {
                                string queueName = "ServiceBusQueue" + nomeOrigem_old;
                                string connName = "ServiceBusConnectionString" + nomeOrigem_old;
                                await GravarServiceBusList(connName, queueName, messages);
                                messages = new List<Message>();
                            }
                            nomeOrigem_old = response.Origem;
                        }
                        else
                        {
                            failed++;
                            logErro.Add(contador, $"Status: {response.Status.ToString()} | Mensagem: {item}");
                        }

                        Console.WriteLine($"\n\n\t\t Processo iniado: {inicio} | Linha: {contador.ToString()} de {totalLinhas} | Total de sucesso: {sucesso} | Total de erros: {failed} | Status: {response.Status.ToString()}");
                    }
                }

                if (messages.Count > 0)
                {
                    string queueName = "ServiceBusQueue" + nomeOrigem_old;
                    string connName = "ServiceBusConnectionString" + nomeOrigem_old;
                    await GravarServiceBusList(connName, queueName, messages);
                }

                var final = DateTime.Now;
                Console.WriteLine($"\n\n\t\t Processo iniado: {inicio} e finalizado {final}");

                GravarLogText(logErro, endPoint);
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        //Realiza a chamada do Endpoint da integração Siebel - Apenas para tratar os Contatos da Susep
        public static async void EnviarSiebelSusep(List<string> listaSusepJson, List<string> listaTelefonesJson, string endPoint)
        {
            try
            {
                int contador = 0;
                int totalLinhas = listaSusepJson.Count;
                int sucesso = 0;
                int failed = 0;
                var inicio = DateTime.Now;

                IList<Message> messagesSuseps = new List<Message>();
                IList<Message> messagesTelefones = new List<Message>();
                ServiceBusRetorno response = new ServiceBusRetorno();

                Dictionary<int, string> logErro = new Dictionary<int, string>();
                List<SiebelSusep> listaSuseps = new List<SiebelSusep>();
                List<SiebelTelefone> listaTelefones = new List<SiebelTelefone>();
                HttpClientIntegracaoSiebel httpClient = new HttpClientIntegracaoSiebel();

                //Diserializar o objeto de telefones para criar as listas dentro de cada susep
                foreach (var item in listaTelefonesJson)
                {
                    var telefone = JsonConvert.DeserializeObject<SiebelTelefone>(item);
                    listaTelefones.Add(telefone);
                }

                //Diserializar o objeto de suseps para poder manipular os registros e gerar as listas com os seus telefones
                foreach (var item in listaSusepJson)
                {
                    var susep = JsonConvert.DeserializeObject<SiebelSusep>(item);

                    var telefones = listaTelefones.Where(t => t.IdVinculo.Trim() == susep.SusepPorto.Trim()).ToList();
                    if (telefones.Count > 0)
                    {
                        susep.Telefones = new List<SiebelTelefone>();
                        susep.Telefones.AddRange(telefones);
                    }

                    listaSuseps.Add(susep);
                }

                //Realiza as chamadas do endpoint "suseps"
                foreach (var susep in listaSuseps)
                {
                    Message messageSusep = null;
                    Message messageTelefone = null;
                    contador++;

                    if (contador > 0)
                    {
                        var susepJson = JsonConvert.SerializeObject(susep);
                        response = await httpClient.ApiIntegracaoSiebel(susepJson, endPoint);

                        if (response.Status.ToString() == "200")
                        {
                            sucesso++;

                            Object mensagemSusepObj = response.Mensagem;
                            messageSusep = new Message(Encoding.UTF8.GetBytes(mensagemSusepObj.ToString()));
                            messagesSuseps.Add(messageSusep);
                            if (messagesSuseps.Count >= 200)
                            {
                                string queueName = "ServiceBusQueueSusep";
                                string connName = "ServiceBusConnectionStringSusep";
                                await GravarServiceBusList(connName, queueName, messagesSuseps);
                                messagesSuseps = new List<Message>();
                            }

                            foreach (var telefone in response.Telefones)
                            {
                                Object mensagemTelefoneObj = telefone;
                                messageTelefone = new Message(Encoding.UTF8.GetBytes(mensagemTelefoneObj.ToString()));
                                messagesTelefones.Add(messageTelefone);
                            }

                            if (messagesTelefones.Count >= 200)
                            {
                                string queueName = "ServiceBusQueueTelefoneSusep";
                                string connName = "ServiceBusConnectionStringTelefoneSusep";
                                await GravarServiceBusList(connName, queueName, messagesTelefones);
                                messagesTelefones = new List<Message>();
                            }
                        }
                        else
                        {
                            failed++;
                            logErro.Add(contador, $"Status: {response.Status.ToString()} | Mensagem: {susep}");
                        }

                        Console.WriteLine($"\n\n\t\t Processo iniado: {inicio} | Linha: {contador.ToString()} de {totalLinhas} | Total de sucesso: {sucesso} | Total de erros: {failed} | Status: {response.Status.ToString()}");
                    }
                }

                if (messagesSuseps.Count > 0)
                {
                    string queueName = "ServiceBusQueueSusep";
                    string connName = "ServiceBusConnectionStringSusep";
                    await GravarServiceBusList(connName, queueName, messagesSuseps);
                }
                if (messagesTelefones.Count > 0)
                {
                    string queueName = "ServiceBusQueueTelefoneSusep";
                    string connName = "ServiceBusConnectionStringTelefoneSusep";
                    await GravarServiceBusList(connName, queueName, messagesTelefones);
                }

                var final = DateTime.Now;
                Console.WriteLine($"\n\n\t\t Processo iniado: {inicio} e finalizado {final}");

                GravarLogText(logErro, endPoint);
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        //Realiza a chamada do Endpoint da integração Siebel - Apenas para tratar os Contatos da Susep
        public static async void EnviarSiebelContatos(List<string> listaContatosJson, List<string> listaTelefonesJson, string endPoint)
        {
            try
            {
                int contador = 0;
                int sucesso = 0;
                int failed = 0;
                var inicio = DateTime.Now;

                ServiceBusRetorno response = new ServiceBusRetorno();
                IList<Message> messagesContatos = new List<Message>();
                IList<Message> messagesTelefones = new List<Message>();
                var listaSusepPorto = new List<string>();
                Dictionary<int, string> logErro = new Dictionary<int, string>();
                List<SiebelContatoSusep> listaContatos = new List<SiebelContatoSusep>();
                List<SiebelTelefone> listaTelefones = new List<SiebelTelefone>();
                HttpClientIntegracaoSiebel httpClient = new HttpClientIntegracaoSiebel();

                Console.WriteLine($"Preparando a lista de telefones {DateTime.Now}...");
                //Diserializar o objeto de telefones para criar as listas dentro de cada susep
                foreach (var telefoneJson in listaTelefonesJson)
                {
                    var telefone = JsonConvert.DeserializeObject<SiebelTelefone>(telefoneJson);
                    listaTelefones.Add(telefone);
                }

                var indice = 0;
                Console.WriteLine($"Preparando a lista de contatos para processar {DateTime.Now}...");
                //Diserializar o objeto de conatao para poder manipular os registros e gerar as listas por [SusepPorto]
                foreach (var contatoJson in listaContatosJson)
                {
                    indice++;
                    try
                    {
                        var contato = JsonConvert.DeserializeObject<SiebelContatoSusep>(contatoJson);
                        var telefones = listaTelefones.Where(t => t.IdVinculo.Trim() == contato.RowId.Trim()).ToList();
                        if (telefones.Count > 0)
                        {
                            contato.Telefones = new List<SiebelTelefone>();
                            contato.Telefones.AddRange(telefones);
                        }

                        //Cria uma lista com as SusepPorto para selecionar os contatos.
                        if (!listaSusepPorto.Contains(contato.SusepPorto))
                            listaSusepPorto.Add(contato.SusepPorto);

                        listaContatos.Add(contato);
                    }
                    catch (Exception ex)
                    {
                        logErro.Add(indice, $" | Mensagem: {contatoJson} | Mensagem de erro: {ex.Message}");
                    }
                }

                GravarLogText(logErro, "Logs-de-leitura-do-csv-e-preparacao-dos-dados-contatos");
                logErro = new Dictionary<int, string>();
                int totalLinhas = listaContatos.Count;

                Console.WriteLine($"Iniciando o processo dos contatos {DateTime.Now}...");
                //Realiza as chamadas do endpoint "contatossuseps"
                foreach (var susepPorto in listaSusepPorto)
                {
                    Message messageContato = null;
                    Message messageTelefone = null;

                    if (contador >= 0)
                    {
                        var contatos = listaContatos.Where(c => c.SusepPorto.Trim() == susepPorto.Trim()).ToList();
                        if (contatos.Count > 0)
                        {
                            var contatosJson = JsonConvert.SerializeObject(contatos);
                            response = await httpClient.ApiIntegracaoSiebel(contatosJson, endPoint);

                            if (response.Status.ToString() == "200")
                            {
                                foreach (var contato in response.Contatos)
                                {
                                    sucesso++;
                                    contador++;

                                    Object mensagemSusepObj = contato;
                                    messageContato = new Message(Encoding.UTF8.GetBytes(mensagemSusepObj.ToString()));
                                    messagesContatos.Add(messageContato);
                                }
                                if (messagesContatos.Count >= 150)
                                {
                                    string queueName = "ServiceBusQueueContato";
                                    string connName = "ServiceBusConnectionStringContato";
                                    await GravarServiceBusList(connName, queueName, messagesContatos);
                                    messagesContatos = new List<Message>();
                                }

                                foreach (var telefone in response.Telefones)
                                {
                                    Object mensagemTelefoneObj = telefone;
                                    messageTelefone = new Message(Encoding.UTF8.GetBytes(mensagemTelefoneObj.ToString()));
                                    messagesTelefones.Add(messageTelefone);
                                }

                                if (messagesTelefones.Count >= 200)
                                {
                                    string queueName = "ServiceBusQueueTelefoneContato";
                                    string connName = "ServiceBusConnectionStringTelefoneContato";
                                    await GravarServiceBusList(connName, queueName, messagesTelefones);
                                    messagesTelefones = new List<Message>();
                                }

                                foreach (var mensagemErro in response.MensagensErros)
                                {
                                    contador++;
                                    failed++;
                                    logErro.Add(failed, $"Status: {response.Status.ToString()} | Mensagem: {mensagemErro}");
                                }
                            }
                            else
                            {
                                if (response.MensagensErros.Count > 0)
                                {
                                    foreach (var mensagemErro in response.MensagensErros)
                                    {
                                        contador++;

                                        failed++;
                                        logErro.Add(failed, $"Status: {response.Status.ToString()} | Mensagem: {mensagemErro}");
                                    }
                                }
                                else
                                {
                                    contador += contatos.Count;

                                    failed++;
                                    logErro.Add(failed, $"Status: {response.Status.ToString()} | Mensagem: {response.MensagemErro}");
                                }
                            }
                        }
                    }

                    Console.WriteLine($"\n\n\t\t Processo iniado: {inicio} | Linha: {contador.ToString()} de {totalLinhas} | Total de sucesso: {sucesso} | Total de erros: {failed} | Status: {response.Status.ToString()}");
                }

                if (messagesContatos.Count > 0)
                {
                    string queueName = "ServiceBusQueueContato";
                    string connName = "ServiceBusConnectionStringContato";
                    await GravarServiceBusList(connName, queueName, messagesContatos);
                }
                if (messagesTelefones.Count > 0)
                {
                    string queueName = "ServiceBusQueueTelefoneContato";
                    string connName = "ServiceBusConnectionStringTelefoneContato";
                    await GravarServiceBusList(connName, queueName, messagesTelefones);
                }

                var final = DateTime.Now;
                Console.WriteLine($"\n\n\t\t Processo iniado: {inicio} e finalizado {final}");

                GravarLogText(logErro, endPoint);
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        public static async Task GravarServiceBusList(string connName, string queueName, IList<Message> messages)
        {
            var appSettings = System.Configuration.ConfigurationManager.AppSettings;
            string sbConnectionString = appSettings[connName] ?? string.Empty;
            string sbQueueName = appSettings[queueName] ?? string.Empty;

            QueueClient queueClientHolding = new QueueClient(sbConnectionString, sbQueueName);
            await queueClientHolding.SendAsync(messages);
            await queueClientHolding.CloseAsync();
        }

        public static void GravarLogText(Dictionary<int, string> logErro, string endPoint)
        {
            string[] lines = new string[logErro.Count];
            int indice = 0;
            foreach (var log in logErro)
            {
                string logTxt = log.Key.ToString() + " - " + log.Value;
                lines[indice] = logTxt;
                indice++;
            }

            string folderName = @"D:\ProjetoPorto\LogAPIIntegracao\";
            string fileName = endPoint.Replace("/", "-") + ".txt";
            string fullPath = folderName + fileName;

            if (!Directory.Exists(folderName))
            {
                Directory.CreateDirectory(folderName);
            }
            if (File.Exists(fullPath))
            {
                File.Delete(fullPath);
            }

            File.WriteAllLines(folderName + fileName, lines);
        }

        #endregion

        ///////////////////////////////////////////////////////////////////
    }

    //Apenas para trabalhar o CSV dos ContatosSusep
    public class SiebelSusep
    {
        public int CodigoVP { get; set; }
        public int CodigoDG { get; set; }
        public int CodigoDC { get; set; }
        public int CodigoGS { get; set; }
        public int CodigoSucursal { get; set; }
        public int CodigoGR { get; set; }
        public int CodigoGC { get; set; }
        public int? CodigoConsolidado { get; set; }
        public int? CodigoHolding { get; set; }
        public int? CodigoGrupo { get; set; }
        public string CodigoSusepPrincipal { get; set; }
        public string SusepPorto { get; set; }
        public string Corretora { get; set; }
        public string Status { get; set; }
        public string TipoPessoa { get; set; }
        public string Email { get; set; }
        public string Endereco { get; set; }
        public string Complemento { get; set; }
        public string Bairro { get; set; }
        public string Cidade { get; set; }
        public string Cep { get; set; }
        public string Estado { get; set; }
        public Int64? Cpf { get; set; }
        public string PesCod { get; set; }
        public Int64? Cnpj { get; set; }
        public string TipoConta { get; set; }
        public string SusepPrincipal { get; set; }
        public string StatusMulticanal { get; set; }
        public DateTime? DataEfetivacao { get; set; }
        public DateTime? DataStatus { get; set; }
        public string BloqueioProducao { get; set; }
        public string BloqueioFenacor { get; set; }
        public string BloqueioComissoes { get; set; }
        public string BloqueioInativos { get; set; }
        public string BloqueioJustificativa { get; set; }
        public int TipoCRUD { get; set; }

        public List<SiebelTelefone> Telefones { get; set; }
    }

    public class SiebelContatoSusep
    {
        public string RowId { get; set; }
        public string SusepPorto { get; set; }
        public Int64? Cpf { get; set; }
        public string Tipo { get; set; }
        public string SubTipo { get; set; }
        public string Nome { get; set; }
        public string Sobrenome { get; set; }
        public string PesCod { get; set; }
        public DateTime? DataNascimento { get; set; }
        public string Email { get; set; }
        public string Endereco { get; set; }
        public string Complemento { get; set; }
        public string Bairro { get; set; }
        public string Cidade { get; set; }
        public string Estado { get; set; }
        public string Cep { get; set; }
        public string StatusRadar { get; set; }
        public int TipoCRUD { get; set; }

        public List<SiebelTelefone> Telefones { get; set; }
    }

    public class SiebelTelefone
    {
        public string IdVinculo { get; set; }
        public string RowId { get; set; }
        public string DDDTelefone { get; set; }
        public string TipoTelefone { get; set; }
        public string TelefonePrincipal { get; set; }
        public int TipoCRUD { get; set; }
    }

    public class ServiceBusRetorno
    {
        public string Mensagem { get; set; }
        public List<string> Contatos { get; set; } = new List<string>();
        public List<string> Telefones { get; set; }
        public string Origem { get; set; }
        public string Status { get; set; }
        public string MensagemErro { get; set; }
        public List<string> MensagensErros { get; set; } = new List<string>();
    }
}
