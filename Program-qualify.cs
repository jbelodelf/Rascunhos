using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using PortoSeguro.GestaoLeads.AzureTable.DAL.OrcamentoSynapseBI;
using PortoSeguro.GestaoLeads.AzureTable.Models.IRepository.OrcamentoSynapseBI;
using PortoSeguro.GestaoLeads.AzureTable.Models.OrcamentoSynapseBI;
using PortoSeguro.GestaoLeads.Principal.Domain.Enum;
using PortoSeguro.GestaoLeads.Principal.Domain.Model;
using PortoSeguro.GestaoLeads.Principal.Domain.Service;
using PortoSeguro.GestaoLeads.Principal.ValueObject;
using PortoSeguro.WebJob.HierarquiaComercial;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace PortoSeguro.WebJob.QualificadorLeads
{
    public class Program
    {
        #region Dados para conexão do CRM
        public static string _orgDynamics;
        public static IOrganizationService _provider;
        public static ConfiguracaoConexaoCrmVO configuracaoCrm = null;
        public static Guid? proprietarioId = null;
        //public static List<QualificacaoLeads> listaRegrasQualificacaoLeads = null;
        public static List<Blacklist> blacklists = null;
        public static List<ScoreSerasaCache> listaScoreSerasa = null;

        public static List<GestaoLeads.Principal.Domain.Model.Produto> produtos = null;
        public static List<GestaoLeads.Principal.Domain.Model.Corretora> corretoras = null;
        public static List<GestaoLeads.Principal.Domain.Model.Pessoa> pessoas = null;
        public static List<GestaoLeads.Principal.Domain.Model.QualificacaoLeads> regrasQualificador = null;
        #endregion

        #region Variaves para a execução em threads
        //---------------------------------------------------------------------------
        private static List<OrcamentoSynapse> orcamentos = null;
        private static List<OrcamentoSynapse> orcamentosDuplicidades = null;
        private static List<OrcamentoSynapse> orcamentosSimples = null;
        //---------------------------------------------------------------------------
        private static IOrcamentoSynapse _orcamentoSynapse = null;
        private static ILogQualificadorLeads _logQualificador = null;
        private static List<string> listaCpfCnpjsProssesso01 = new List<string>();
        private static List<string> listaCpfCnpjsProssesso02 = new List<string>();
        private static List<string> listaCpfCnpjsProssesso03 = new List<string>();
        private static List<string> listaCpfCnpjsProssesso04 = new List<string>();
        private static List<string> listaCpfCnpjsProssesso05 = new List<string>();
        private static List<string> listaCpfCnpjsProssesso06 = new List<string>();
        private static List<string> listaCpfCnpjsProssesso07 = new List<string>();
        private static List<string> listaCpfCnpjsProssesso08 = new List<string>();
        private static List<string> listaCpfCnpjsProssesso09 = new List<string>();
        private static List<string> listaCpfCnpjsProssesso10 = new List<string>();
        //---------------------------------------------------------------------------
        #endregion

        static void Main(string[] args)
        {
            #region Cria a conexção do CRM
            var appSettings = System.Configuration.ConfigurationManager.AppSettings;
            _orgDynamics = appSettings["orgDynamics"] ?? string.Empty;
            configuracaoCrm = new ConfiguracaoConexaoCrmVO();
            configuracaoCrm.ClientId = SDKore.Util.Cryptography.Decrypt(appSettings["Client-ID"]) ?? string.Empty;
            configuracaoCrm.TenantId = SDKore.Util.Cryptography.Decrypt(appSettings["Tenant-ID"]) ?? string.Empty;
            configuracaoCrm.ClientSecret = SDKore.Util.Cryptography.Decrypt(appSettings["Client-Secret"]) ?? string.Empty;
            configuracaoCrm.ResourceUrl = appSettings["Resource-URL"] ?? string.Empty;
            configuracaoCrm.ServiceRoot = appSettings["Service-Root"] ?? string.Empty;
            configuracaoCrm.ServiceUrl = appSettings["Service-URL"] ?? string.Empty;

            UtilCriarConexao utilCriarConexao = new UtilCriarConexao(configuracaoCrm);
            _provider = utilCriarConexao.GerarOrganizationByAzureApp();
            #endregion

            try
            {
                _orcamentoSynapse = new RepositoryOrcamentoSynapse();
                _logQualificador = new RepositoryLogQualificadorLeads();

                //Garregar a primeira lista dos Orçamentos para processar
                CarregarOrcamentos().GetAwaiter().GetResult();
                if (orcamentos.Count > 0)
                {
                    CarregarAsListasDoCRM().GetAwaiter().GetResult();
                    ExecutarProcesso().GetAwaiter().GetResult();

                    // Verificar se ainda ficou algum Orçamentos para processar, 
                    // reiniciar o processamento reaproveirando todas as lista que foram carregadas no início
                    while (true)
                    {
                        //Reprocessar enquanto houver orçamentos com IsQualificadorRodou=false;
                        CarregarOrcamentos().GetAwaiter().GetResult();
                        if (orcamentos.Count > 0)
                        {
                            ExecutarProcesso().GetAwaiter().GetResult();
                        }
                        else
                        {
                            break;
                        }
                    }
                }

                Console.WriteLine($"\t Processo finalizado ");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\t Erro: {ex.Message} ");
                Console.WriteLine($"\t Processo finalizado ");
            }
        }

        private static async Task CarregarOrcamentos()
        {
            Console.WriteLine($"\t Iniciado a requisição dos Orçamentos {DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff")} ");
            orcamentos = _orcamentoSynapse.ListOrcamentos().ToList();
            Console.WriteLine($"\t Requisição finalizada {DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff")}, totatl de Orçamentos: {orcamentos.Count} ");

            //if (orcamentos.Count > 0)
            //{
            //    orcamentosDuplicidades = new List<OrcamentoSynapse>();
            //    orcamentosSimples = new List<OrcamentoSynapse>();
            //    var listaOrdenada = orcamentos.OrderBy(o => o.PessoaCpfCnpj).ThenBy(o => o.ChaveDuplicidade).ThenBy(o => o.NumeroOrcamentoPrincipal).ToList();
            //    foreach (var orcamento in listaOrdenada)
            //    {
            //        if (orcamentosSimples.Any(o => o.ChaveDuplicidade.Trim() == orcamento.ChaveDuplicidade.Trim() && o.PessoaCpfCnpj.Trim() == orcamento.PessoaCpfCnpj.Trim()))
            //        {
            //            orcamentosDuplicidades.Add(orcamento);
            //        }
            //        else
            //        {
            //            orcamentosSimples.Add(orcamento);
            //        }
            //    }
            //}
        }

        private static async Task CarregarAsListasDoCRM()
        {
             if (orcamentos.Count > 0)
             {
                #region Carregar os dados do CRM
                Console.WriteLine($"\t Iniciado a requisição da Lista de Produtos {DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff")} ");
                GestaoLeads.Principal.Domain.Model.Produto produtoEntity = new GestaoLeads.Principal.Domain.Model.Produto(_orgDynamics, false, _provider);
                produtos = produtoEntity.Listar().ToList();
                Console.WriteLine($"\t Finalizado a Requisição da Lista de Produtos {DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff")}, total: {produtos.Count} ");

                Console.WriteLine($"\t Iniciado a requisição da Lista de Regras Qualificador {DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff")} ");
                GestaoLeads.Principal.Domain.Model.QualificacaoLeads qualificador = new GestaoLeads.Principal.Domain.Model.QualificacaoLeads(_orgDynamics, false, _provider);
                regrasQualificador = qualificador.Listar().ToList();
                Console.WriteLine($"\t Finalizado a Requisição da Lista de Regras Qualificador {DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff")}, total: {regrasQualificador.Count} ");

                //Console.WriteLine($"\t Iniciado a requisição da Lista de Corretoras {DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff")} ");
                //GestaoLeads.Principal.Domain.Model.Corretora corretoraEntity = new GestaoLeads.Principal.Domain.Model.Corretora(_orgDynamics, false, _provider);
                //corretoras = corretoraEntity.ListaCorretoraPorSusep();
                //Console.WriteLine($"\t Finalizado a Requisição da Lista de Corretoras {DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff")}, total: {corretoras.Count} ");

                Console.WriteLine($"\t Iniciado a requisição da Lista de Pessoas {DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff")} ");
                GestaoLeads.Principal.Domain.Model.Pessoa pessoa = new GestaoLeads.Principal.Domain.Model.Pessoa(_orgDynamics, false, _provider);
                pessoas = pessoa.ListarTodos();
                Console.WriteLine($"\t Finalizado a Requisição da Lista de Pessoas {DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff")}, total: {pessoas.Count} ");

                Console.WriteLine($"\t Iniciado a requisição dos BlackList {DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff")} ");
                Blacklist blacklist = new Blacklist(_orgDynamics, false, _provider);
                blacklists = blacklist.Listar();
                Console.WriteLine($"\t Requisição finalizada {DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff")}, totatl de BlockList: {blacklists.Count} ");

                proprietarioId = new Guid(new Configuracao(_orgDynamics, false, _provider).ObterPor("PessoaEquipeProprietaria")?.Valor);

                listaScoreSerasa = new List<ScoreSerasaCache>();
                #endregion
             }
        }

        private static async Task ExecutarProcesso()
        {
            //bool rodouSimples = false;
            //bool rodouDuplicidades = false;
            if (orcamentos.Count > 0)
            {
                //RodarDuplicidades:
                #region Distribuição dos Cpfs na Threads
                List<string> listaCpfCnpjs = orcamentos.GroupBy(o => o.PessoaCpfCnpj).Select(x => x.FirstOrDefault().PessoaCpfCnpj).ToList();
                List<string> listaDistinctCpfCnpjs = listaCpfCnpjs.Distinct().ToList();

                //List<string> listaDistinctCpfCnpjs = new List<string>();
                //if (!rodouSimples)
                //{
                //    List<string> listaCpfCnpjs = orcamentosSimples.GroupBy(o => o.PessoaCpfCnpj).Select(x => x.FirstOrDefault().PessoaCpfCnpj).ToList();
                //    listaDistinctCpfCnpjs = listaCpfCnpjs.Distinct().ToList();
                //    rodouSimples = true;
                //    orcamentos = orcamentosSimples;
                //}
                //else
                //{
                //    List<string> listaCpfCnpjs = orcamentosDuplicidades.GroupBy(o => o.PessoaCpfCnpj).Select(x => x.FirstOrDefault().PessoaCpfCnpj).ToList();
                //    listaDistinctCpfCnpjs = listaCpfCnpjs.Distinct().ToList();
                //    rodouDuplicidades = true;
                //    orcamentos = orcamentosDuplicidades;
                //}

                if (listaDistinctCpfCnpjs.Count > 0)
                {
                    //Distribuir os CPFs por processo
                    var totalCpfs = listaDistinctCpfCnpjs.Count;
                    //var cotaCpfs = totalCpfs;
                    var cotaCpfs = totalCpfs / 5;
                    listaCpfCnpjsProssesso01 = listaDistinctCpfCnpjs.Take(cotaCpfs).ToList();
                    listaCpfCnpjsProssesso02 = listaDistinctCpfCnpjs.Skip(cotaCpfs).Take(cotaCpfs).ToList();
                    listaCpfCnpjsProssesso03 = listaDistinctCpfCnpjs.Skip((cotaCpfs * 2)).Take(cotaCpfs).ToList();
                    listaCpfCnpjsProssesso04 = listaDistinctCpfCnpjs.Skip((cotaCpfs * 3)).Take(cotaCpfs).ToList();
                    listaCpfCnpjsProssesso05 = listaDistinctCpfCnpjs.Skip((cotaCpfs * 4)).Take((totalCpfs - (cotaCpfs * 4))).ToList();

                    //listaCpfCnpjsProssesso05 = listaDistinctCpfCnpjs.Skip((cotaCpfs * 4)).Take(cotaCpfs).ToList();
                    //listaCpfCnpjsProssesso06 = listaDistinctCpfCnpjs.Skip((cotaCpfs * 5)).Take(cotaCpfs).ToList();
                    //listaCpfCnpjsProssesso07 = listaDistinctCpfCnpjs.Skip((cotaCpfs * 6)).Take(cotaCpfs).ToList();
                    //listaCpfCnpjsProssesso08 = listaDistinctCpfCnpjs.Skip((cotaCpfs * 7)).Take(cotaCpfs).ToList();
                    //listaCpfCnpjsProssesso09 = listaDistinctCpfCnpjs.Skip((cotaCpfs * 8)).Take(cotaCpfs).ToList();
                    //listaCpfCnpjsProssesso10 = listaDistinctCpfCnpjs.Skip((cotaCpfs * 9)).Take((totalCpfs - (cotaCpfs * 9))).ToList();
                    #endregion

                    #region Ativação das Thraeds
                    //--------------Threads Multithreading----------------\\
                    Thread thread01 = new Thread(() => { Processo01(); });
                    Thread thread02 = new Thread(() => { Processo02(); });
                    Thread thread03 = new Thread(() => { Processo03(); });
                    Thread thread04 = new Thread(() => { Processo04(); });
                    Thread thread05 = new Thread(() => { Processo05(); });
                    //Thread thread06 = new Thread(() => { Processo06(); });
                    //Thread thread07 = new Thread(() => { Processo07(); });
                    //Thread thread08 = new Thread(() => { Processo08(); });
                    //Thread thread09 = new Thread(() => { Processo09(); });
                    //Thread thread10 = new Thread(() => { Processo10(); });
                    //----------------------OU----------------------------\\
                    //Thread thread01 = new Thread(() => { ProcessarThreads(listaCpfCnpjsProssesso01, "01"); });
                    //Thread thread02 = new Thread(() => { ProcessarThreads(listaCpfCnpjsProssesso02, "02"); });
                    //Thread thread03 = new Thread(() => { ProcessarThreads(listaCpfCnpjsProssesso03, "03"); });
                    //Thread thread04 = new Thread(() => { ProcessarThreads(listaCpfCnpjsProssesso04, "04"); });
                    //Thread thread05 = new Thread(() => { ProcessarThreads(listaCpfCnpjsProssesso05, "05"); });
                    //Thread thread06 = new Thread(() => { ProcessarThreads(listaCpfCnpjsProssesso06, "06"); });
                    //Thread thread07 = new Thread(() => { ProcessarThreads(listaCpfCnpjsProssesso07, "07"); });
                    //Thread thread08 = new Thread(() => { ProcessarThreads(listaCpfCnpjsProssesso08, "08"); });
                    //Thread thread09 = new Thread(() => { ProcessarThreads(listaCpfCnpjsProssesso09, "09"); });
                    //Thread thread10 = new Thread(() => { ProcessarThreads(listaCpfCnpjsProssesso10, "10"); });
                    //----------------------------------------------------\\
                    thread01.Start();
                    thread02.Start();
                    thread03.Start();
                    thread04.Start();
                    thread05.Start();
                    //thread06.Start();
                    //thread07.Start();
                    //thread08.Start();
                    //thread09.Start();
                    //thread10.Start();
                    //----------------------------------------------------\\
                    thread01.Join();
                    thread02.Join();
                    thread03.Join();
                    thread04.Join();
                    thread05.Join();
                    //thread06.Join();
                    //thread07.Join();
                    //thread08.Join();
                    //thread09.Join();
                    //thread10.Join();
                    //----------------------------------------------------\\
                    #endregion

                    //if (!rodouSimples || !rodouDuplicidades)
                    //{
                    //    goto RodarDuplicidades;
                    //}
                }
            }
        }

        #region Metódos para processamento as Threads
        public static void Processo01()
        {
            QualificadorLeads qualificadorProcesso1 = null;
            DateTime connextioCriadaEm = DateTime.Now;
            int contagem = 0;
            int contagemCpf = 0;
            int totalQualidicado = 0;
            int totalDesqualidicado = 0;
            int totalDuplicidade = 0;
            int totalAvaliar = 0;
            int totalNutrir = 0;

            foreach (var proc1 in listaCpfCnpjsProssesso01)
            {
                contagemCpf++;
                List<OrcamentoSynapse> listaOrcamentos = orcamentos.Where(o => o.PessoaCpfCnpj.Trim() == proc1.Trim()).ToList();
                if (orcamentos.Count > 0)
                {
                    foreach (var orcam in listaOrcamentos.OrderBy(o => o.NumeroItemOrcamento))
                    {
                        ReNewConnection:
                        try
                        {
                            var tempoConnetion = (DateTime.Now - connextioCriadaEm);
                            if (qualificadorProcesso1 == null)
                            {
                                connextioCriadaEm = DateTime.Now;
                                qualificadorProcesso1 = new QualificadorLeads();
                            }

                            RetornoQualificador atualizarOrcamento = qualificadorProcesso1.OrcamentoQualificarLead(orcam).GetAwaiter().GetResult();

                            //Valida se gerou erro de exception na execução do processo para o CRM, segue para o próximo registro, deixando esse registro no Cosmos para ser reprocessado.
                            if (atualizarOrcamento.OrcamentoSynapse.LogQualificador.Contains("ERRO DE EXCEÇÃO:"))
                                continue;

                            atualizarOrcamento.FinalProcesso = DateTime.Now;
                            contagem++;
                            if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Duplicidade)
                                totalDuplicidade++;
                            else if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Desqualificar)
                                totalDesqualidicado++;
                            else if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Qualificar)
                                totalQualidicado++;
                            else if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Avaliar)
                                totalAvaliar++;
                            else if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Nutrir)
                                totalNutrir++;

                            if (tempoConnetion.TotalMinutes > 30)
                            {
                                qualificadorProcesso1 = null;
                            }

                            Console.WriteLine($"  Processo 1: => Linha {contagemCpf} de {listaCpfCnpjsProssesso01.Count}, Processado: {contagem}, Qualificados: {totalQualidicado}, Desqualificados: {totalDesqualidicado}, Duplicados: {totalDuplicidade}, Avaliar: {totalAvaliar}, Nutrir: {totalNutrir} - {DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff")}");

                            if (atualizarOrcamento == null)
                            {
                                Console.WriteLine($"\t Esse orçamento deu erro no processamento ");
                            }
                            else
                            {
                                try
                                {
                                    Task.Factory.StartNew(() => {
                                        //Atualizar o Orçamento
                                        atualizarOrcamento.OrcamentoSynapse.DataModificacao = DateTime.Now;
                                        _orcamentoSynapse.Update(atualizarOrcamento.OrcamentoSynapse);

                                        //Gravar o Log no Azure Table
                                        LogQualificadorLeads logQualificadorLead = new LogQualificadorLeads("LogDoQualificadorLead", Guid.NewGuid().ToString())
                                        {
                                            OrcamentoId = atualizarOrcamento.OrcamentoSynapse.InternalId,
                                            TipoOperacao = Enum.GetName(typeof(ResultadoQualificacao), atualizarOrcamento.ResultadoProcesso),
                                            LogDescricao = atualizarOrcamento.OrcamentoSynapse.LogQualificador,
                                            InicioProcesso = atualizarOrcamento.InicioProcesso,
                                            FinalProcesso = atualizarOrcamento.FinalProcesso,
                                            TempoTotalProcessamento = Convert.ToInt32((atualizarOrcamento.FinalProcesso - atualizarOrcamento.InicioProcesso).TotalMilliseconds),
                                            DataCriacao = DateTime.Now,
                                            //--------------------------------------------------------------------------------
                                            ContatoFoiCriado = atualizarOrcamento.ContatoFoiCriado,
                                            TempoExecucaoContato = Convert.ToInt32((atualizarOrcamento.FinalCriarContato - atualizarOrcamento.InicioCriarContato).TotalMilliseconds),
                                            TempoCriarLead = Convert.ToInt32((atualizarOrcamento.FinalCriarLead - atualizarOrcamento.InicioCriarLead).TotalMilliseconds),
                                            TempoCriarOportunidade = Convert.ToInt32((atualizarOrcamento.FinalCriarOpp - atualizarOrcamento.InicioCriarOpp).TotalMilliseconds),
                                            TempoAtualizarDuplicidade = Convert.ToInt32((atualizarOrcamento.FinalAtualizarDuplicidade - atualizarOrcamento.InicioAtualizarDuplicidade).TotalMilliseconds),
                                            //--------------------------------------------------------------------------------
                                        };

                                        _logQualificador.Create(logQualificadorLead);
                                    });
                                }
                                catch (Exception ex)
                                {
                                    throw (new ArgumentException(ex.Message));
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            if (ex.Message.Contains("Bearer authorization_uri"))
                            {
                                qualificadorProcesso1 = null;
                                goto ReNewConnection;
                            }
                            Console.WriteLine($"\t Erro no processo-1: {ex.Message} ");
                        }
                    }
                }

            }
        }

        public static void Processo02()
        {
            QualificadorLeads qualificadorProcesso2 = null;
            DateTime connextioCriadaEm = DateTime.Now.AddHours(-1);
            int contagem = 0;
            int contagemCpf = 0;
            int totalQualidicado = 0;
            int totalDesqualidicado = 0;
            int totalDuplicidade = 0;
            int totalAvaliar = 0;
            int totalNutrir = 0;

            foreach (var proc2 in listaCpfCnpjsProssesso02)
            {
                contagemCpf++;
                List<OrcamentoSynapse> listaOrcamentos = orcamentos.Where(o => o.PessoaCpfCnpj.Trim() == proc2.Trim()).ToList();
                if (orcamentos.Count > 0)
                {
                    foreach (var orcam in listaOrcamentos.OrderBy(o => o.NumeroItemOrcamento))
                    {
                        ReNewConnection:
                        try
                        {
                            var tempoConnetion = (DateTime.Now - connextioCriadaEm);
                            if (qualificadorProcesso2 == null)
                            {
                                connextioCriadaEm = DateTime.Now;
                                qualificadorProcesso2 = new QualificadorLeads();
                            }

                            RetornoQualificador atualizarOrcamento = qualificadorProcesso2.OrcamentoQualificarLead(orcam).GetAwaiter().GetResult();

                            //Valida se gerou erro de exception na execução do processo para o CRM, segue para o próximo registro, deixando esse registro no Cosmos para ser reprocessado.
                            if (atualizarOrcamento.OrcamentoSynapse.LogQualificador.Contains("ERRO DE EXCEÇÃO:"))
                                continue;

                            atualizarOrcamento.FinalProcesso = DateTime.Now;
                            contagem++;
                            if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Duplicidade)
                                totalDuplicidade++;
                            else if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Desqualificar)
                                totalDesqualidicado++;
                            else if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Qualificar)
                                totalQualidicado++;
                            else if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Avaliar)
                                totalAvaliar++;
                            else if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Nutrir)
                                totalNutrir++;

                            if (tempoConnetion.TotalMinutes > 30)
                            {
                                qualificadorProcesso2 = null;
                            }

                            Console.WriteLine($"  Processo 2: => Linha {contagemCpf} de {listaCpfCnpjsProssesso02.Count}, Processado: {contagem}, Qualificados: {totalQualidicado}, Desqualificados: {totalDesqualidicado}, Duplicados: {totalDuplicidade}, Avaliar: {totalAvaliar}, Nutrir: {totalNutrir} - {DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff")}");

                            if (atualizarOrcamento == null)
                            {
                                Console.WriteLine($"\t Esse orçamento deu erro no processamento ");
                            }
                            else
                            {
                                try
                                {
                                    Task.Factory.StartNew(() => {
                                        //Atualizar o Orçamento
                                        atualizarOrcamento.OrcamentoSynapse.DataModificacao = DateTime.Now;
                                        _orcamentoSynapse.Update(atualizarOrcamento.OrcamentoSynapse);

                                        //Gravar o Log no Azure Table
                                        LogQualificadorLeads logQualificadorLead = new LogQualificadorLeads("LogDoQualificadorLead", Guid.NewGuid().ToString())
                                        {
                                            OrcamentoId = atualizarOrcamento.OrcamentoSynapse.InternalId,
                                            TipoOperacao = Enum.GetName(typeof(ResultadoQualificacao), atualizarOrcamento.ResultadoProcesso),
                                            LogDescricao = atualizarOrcamento.OrcamentoSynapse.LogQualificador,
                                            InicioProcesso = atualizarOrcamento.InicioProcesso,
                                            FinalProcesso = atualizarOrcamento.FinalProcesso,
                                            TempoTotalProcessamento = Convert.ToInt32((atualizarOrcamento.FinalProcesso - atualizarOrcamento.InicioProcesso).TotalMilliseconds),
                                            DataCriacao = DateTime.Now,
                                            //--------------------------------------------------------------------------------
                                            ContatoFoiCriado = atualizarOrcamento.ContatoFoiCriado,
                                            TempoExecucaoContato = Convert.ToInt32((atualizarOrcamento.FinalCriarContato - atualizarOrcamento.InicioCriarContato).TotalMilliseconds),
                                            TempoCriarLead = Convert.ToInt32((atualizarOrcamento.FinalCriarLead - atualizarOrcamento.InicioCriarLead).TotalMilliseconds),
                                            TempoCriarOportunidade = Convert.ToInt32((atualizarOrcamento.FinalCriarOpp - atualizarOrcamento.InicioCriarOpp).TotalMilliseconds),
                                            TempoAtualizarDuplicidade = Convert.ToInt32((atualizarOrcamento.FinalAtualizarDuplicidade - atualizarOrcamento.InicioAtualizarDuplicidade).TotalMilliseconds),
                                            //--------------------------------------------------------------------------------
                                        };

                                        _logQualificador.Create(logQualificadorLead);
                                    });
                                }
                                catch (Exception ex)
                                {
                                    throw (new ArgumentException(ex.Message));
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            if (ex.Message.Contains("Bearer authorization_uri"))
                            {
                                qualificadorProcesso2 = null;
                                goto ReNewConnection;
                            }
                            Console.WriteLine($"\t Erro no processo-2: {ex.Message} ");
                        }
                    }
                }
            }
        }

        public static void Processo03()
        {
            QualificadorLeads qualificadorProcesso3 = null;
            DateTime connextioCriadaEm = DateTime.Now.AddHours(-1);
            int contagem = 0;
            int contagemCpf = 0;
            int totalQualidicado = 0;
            int totalDesqualidicado = 0;
            int totalDuplicidade = 0;
            int totalAvaliar = 0;
            int totalNutrir = 0;

            foreach (var proc3 in listaCpfCnpjsProssesso03)
            {
                contagemCpf++;
                List<OrcamentoSynapse> listaOrcamentos = orcamentos.Where(o => o.PessoaCpfCnpj.Trim() == proc3.Trim()).ToList();
                if (orcamentos.Count > 0)
                {
                    foreach (var orcam in listaOrcamentos.OrderBy(o => o.NumeroItemOrcamento))
                    {
                        ReNewConnection:
                        try
                        {
                            var tempoConnetion = (DateTime.Now - connextioCriadaEm);
                            if (qualificadorProcesso3 == null)
                            {
                                connextioCriadaEm = DateTime.Now;
                                qualificadorProcesso3 = new QualificadorLeads();
                            }

                            RetornoQualificador atualizarOrcamento = qualificadorProcesso3.OrcamentoQualificarLead(orcam).GetAwaiter().GetResult();

                            //Valida se gerou erro de exception na execução do processo para o CRM, segue para o próximo registro, deixando esse registro no Cosmos para ser reprocessado.
                            if (atualizarOrcamento.OrcamentoSynapse.LogQualificador.Contains("ERRO DE EXCEÇÃO:"))
                                continue;

                            atualizarOrcamento.FinalProcesso = DateTime.Now;
                            contagem++;
                            if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Duplicidade)
                                totalDuplicidade++;
                            else if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Desqualificar)
                                totalDesqualidicado++;
                            else if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Qualificar)
                                totalQualidicado++;
                            else if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Avaliar)
                                totalAvaliar++;
                            else if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Nutrir)
                                totalNutrir++;

                            if (tempoConnetion.TotalMinutes > 30)
                            {
                                qualificadorProcesso3 = null;
                            }

                            Console.WriteLine($"  Processo 3: => Linha {contagemCpf} de {listaCpfCnpjsProssesso03.Count}, Processado: {contagem}, Qualificados: {totalQualidicado}, Desqualificados: {totalDesqualidicado}, Duplicados: {totalDuplicidade}, Avaliar: {totalAvaliar}, Nutrir: {totalNutrir} - {DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff")}");

                            if (atualizarOrcamento == null)
                            {
                                Console.WriteLine($"\t Esse orçamento deu erro no processamento ");
                            }
                            else
                            {
                                try
                                {
                                    Task.Factory.StartNew(() => {
                                        //Atualizar o Orçamento
                                        atualizarOrcamento.OrcamentoSynapse.DataModificacao = DateTime.Now;
                                        _orcamentoSynapse.Update(atualizarOrcamento.OrcamentoSynapse);

                                        //Gravar o Log no Azure Table
                                        LogQualificadorLeads logQualificadorLead = new LogQualificadorLeads("LogDoQualificadorLead", Guid.NewGuid().ToString())
                                        {
                                            OrcamentoId = atualizarOrcamento.OrcamentoSynapse.InternalId,
                                            TipoOperacao = Enum.GetName(typeof(ResultadoQualificacao), atualizarOrcamento.ResultadoProcesso),
                                            LogDescricao = atualizarOrcamento.OrcamentoSynapse.LogQualificador,
                                            InicioProcesso = atualizarOrcamento.InicioProcesso,
                                            FinalProcesso = atualizarOrcamento.FinalProcesso,
                                            TempoTotalProcessamento = Convert.ToInt32((atualizarOrcamento.FinalProcesso - atualizarOrcamento.InicioProcesso).TotalMilliseconds),
                                            DataCriacao = DateTime.Now,
                                            //--------------------------------------------------------------------------------
                                            ContatoFoiCriado = atualizarOrcamento.ContatoFoiCriado,
                                            TempoExecucaoContato = Convert.ToInt32((atualizarOrcamento.FinalCriarContato - atualizarOrcamento.InicioCriarContato).TotalMilliseconds),
                                            TempoCriarLead = Convert.ToInt32((atualizarOrcamento.FinalCriarLead - atualizarOrcamento.InicioCriarLead).TotalMilliseconds),
                                            TempoCriarOportunidade = Convert.ToInt32((atualizarOrcamento.FinalCriarOpp - atualizarOrcamento.InicioCriarOpp).TotalMilliseconds),
                                            TempoAtualizarDuplicidade = Convert.ToInt32((atualizarOrcamento.FinalAtualizarDuplicidade - atualizarOrcamento.InicioAtualizarDuplicidade).TotalMilliseconds),
                                            //--------------------------------------------------------------------------------
                                        };

                                        _logQualificador.Create(logQualificadorLead);
                                    });
                                }
                                catch (Exception ex)
                                {
                                    throw (new ArgumentException(ex.Message));
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            if (ex.Message.Contains("Bearer authorization_uri"))
                            {
                                qualificadorProcesso3 = null;
                                goto ReNewConnection;
                            }
                            Console.WriteLine($"\t Erro no processo-3: {ex.Message} ");
                        }
                    }
                }
            }
        }

        public static void Processo04()
        {
            QualificadorLeads qualificadorProcesso4 = null;
            DateTime connextioCriadaEm = DateTime.Now.AddHours(-1);
            int contagem = 0;
            int contagemCpf = 0;
            int totalQualidicado = 0;
            int totalDesqualidicado = 0;
            int totalDuplicidade = 0;
            int totalAvaliar = 0;
            int totalNutrir = 0;

            foreach (var proc4 in listaCpfCnpjsProssesso04)
            {
                contagemCpf++;
                List<OrcamentoSynapse> listaOrcamentos = orcamentos.Where(o => o.PessoaCpfCnpj.Trim() == proc4.Trim()).ToList();
                if (orcamentos.Count > 0)
                {
                    foreach (var orcam in listaOrcamentos.OrderBy(o => o.NumeroItemOrcamento))
                    {
                        ReNewConnection:
                        try
                        {
                            var tempoConnetion = (DateTime.Now - connextioCriadaEm);
                            if (qualificadorProcesso4 == null)
                            {
                                connextioCriadaEm = DateTime.Now;
                                qualificadorProcesso4 = new QualificadorLeads();
                            }

                            RetornoQualificador atualizarOrcamento = qualificadorProcesso4.OrcamentoQualificarLead(orcam).GetAwaiter().GetResult();

                            //Valida se gerou erro de exception na execução do processo para o CRM, segue para o próximo registro, deixando esse registro no Cosmos para ser reprocessado.
                            if (atualizarOrcamento.OrcamentoSynapse.LogQualificador.Contains("ERRO DE EXCEÇÃO:"))
                                continue;

                            atualizarOrcamento.FinalProcesso = DateTime.Now;
                            contagem++;
                            if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Duplicidade)
                                totalDuplicidade++;
                            else if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Desqualificar)
                                totalDesqualidicado++;
                            else if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Qualificar)
                                totalQualidicado++;
                            else if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Avaliar)
                                totalAvaliar++;
                            else if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Nutrir)
                                totalNutrir++;

                            if (tempoConnetion.TotalMinutes > 30)
                            {
                                qualificadorProcesso4 = null;
                            }

                            Console.WriteLine($"  Processo 4: => Linha {contagemCpf} de {listaCpfCnpjsProssesso04.Count}, Processado: {contagem}, Qualificados: {totalQualidicado}, Desqualificados: {totalDesqualidicado}, Duplicados: {totalDuplicidade}, Avaliar: {totalAvaliar}, Nutrir: {totalNutrir} - {DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff")}");

                            if (atualizarOrcamento == null)
                            {
                                Console.WriteLine($"\t Esse orçamento deu erro no processamento ");
                            }
                            else
                            {
                                try
                                {
                                    Task.Factory.StartNew(() => {
                                        //Atualizar o Orçamento
                                        atualizarOrcamento.OrcamentoSynapse.DataModificacao = DateTime.Now;
                                        _orcamentoSynapse.Update(atualizarOrcamento.OrcamentoSynapse);

                                        //Gravar o Log no Azure Table
                                        LogQualificadorLeads logQualificadorLead = new LogQualificadorLeads("LogDoQualificadorLead", Guid.NewGuid().ToString())
                                        {
                                            OrcamentoId = atualizarOrcamento.OrcamentoSynapse.InternalId,
                                            TipoOperacao = Enum.GetName(typeof(ResultadoQualificacao), atualizarOrcamento.ResultadoProcesso),
                                            LogDescricao = atualizarOrcamento.OrcamentoSynapse.LogQualificador,
                                            InicioProcesso = atualizarOrcamento.InicioProcesso,
                                            FinalProcesso = atualizarOrcamento.FinalProcesso,
                                            TempoTotalProcessamento = Convert.ToInt32((atualizarOrcamento.FinalProcesso - atualizarOrcamento.InicioProcesso).TotalMilliseconds),
                                            DataCriacao = DateTime.Now,
                                            //--------------------------------------------------------------------------------
                                            ContatoFoiCriado = atualizarOrcamento.ContatoFoiCriado,
                                            TempoExecucaoContato = Convert.ToInt32((atualizarOrcamento.FinalCriarContato - atualizarOrcamento.InicioCriarContato).TotalMilliseconds),
                                            TempoCriarLead = Convert.ToInt32((atualizarOrcamento.FinalCriarLead - atualizarOrcamento.InicioCriarLead).TotalMilliseconds),
                                            TempoCriarOportunidade = Convert.ToInt32((atualizarOrcamento.FinalCriarOpp - atualizarOrcamento.InicioCriarOpp).TotalMilliseconds),
                                            TempoAtualizarDuplicidade = Convert.ToInt32((atualizarOrcamento.FinalAtualizarDuplicidade - atualizarOrcamento.InicioAtualizarDuplicidade).TotalMilliseconds),
                                            //--------------------------------------------------------------------------------
                                        };

                                        _logQualificador.Create(logQualificadorLead);
                                    });
                                }
                                catch (Exception ex)
                                {
                                    throw (new ArgumentException(ex.Message));
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            if (ex.Message.Contains("Bearer authorization_uri"))
                            {
                                qualificadorProcesso4 = null;
                                goto ReNewConnection;
                            }
                            Console.WriteLine($"\t Erro no processo-4: {ex.Message} ");
                        }
                    }
                }
            }
        }

        public static void Processo05()
        {
            QualificadorLeads qualificadorProcesso5 = null;
            DateTime connextioCriadaEm = DateTime.Now.AddHours(-1);
            int contagem = 0;
            int contagemCpf = 0;
            int totalQualidicado = 0;
            int totalDesqualidicado = 0;
            int totalDuplicidade = 0;
            int totalAvaliar = 0;
            int totalNutrir = 0;

            foreach (var proc5 in listaCpfCnpjsProssesso05)
            {
                contagemCpf++;
                List<OrcamentoSynapse> listaOrcamentos = orcamentos.Where(o => o.PessoaCpfCnpj.Trim() == proc5.Trim()).ToList();
                if (orcamentos.Count > 0)
                {
                    foreach (var orcam in listaOrcamentos.OrderBy(o => o.NumeroItemOrcamento))
                    {
                        ReNewConnection:
                        try
                        {
                            var tempoConnetion = (DateTime.Now - connextioCriadaEm);
                            if (qualificadorProcesso5 == null)
                            {
                                connextioCriadaEm = DateTime.Now;
                                qualificadorProcesso5 = new QualificadorLeads();
                            }

                            RetornoQualificador atualizarOrcamento = qualificadorProcesso5.OrcamentoQualificarLead(orcam).GetAwaiter().GetResult();

                            //Valida se gerou erro de exception na execução do processo para o CRM, segue para o próximo registro, deixando esse registro no Cosmos para ser reprocessado.
                            if (atualizarOrcamento.OrcamentoSynapse.LogQualificador.Contains("ERRO DE EXCEÇÃO:"))
                                continue;

                            atualizarOrcamento.FinalProcesso = DateTime.Now;
                            contagem++;
                            if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Duplicidade)
                                totalDuplicidade++;
                            else if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Desqualificar)
                                totalDesqualidicado++;
                            else if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Qualificar)
                                totalQualidicado++;
                            else if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Avaliar)
                                totalAvaliar++;
                            else if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Nutrir)
                                totalNutrir++;

                            if (tempoConnetion.TotalMinutes > 30)
                            {
                                qualificadorProcesso5 = null;
                            }

                            Console.WriteLine($"  Processo 5: => Linha {contagemCpf} de {listaCpfCnpjsProssesso05.Count}, Processado: {contagem}, Qualificados: {totalQualidicado}, Desqualificados: {totalDesqualidicado}, Duplicados: {totalDuplicidade}, Avaliar: {totalAvaliar}, Nutrir: {totalNutrir} - {DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff")}");

                            if (atualizarOrcamento == null)
                            {
                                Console.WriteLine($"\t Esse orçamento deu erro no processamento ");
                            }
                            else
                            {
                                try
                                {
                                    Task.Factory.StartNew(() => {
                                        //Atualizar o Orçamento
                                        atualizarOrcamento.OrcamentoSynapse.DataModificacao = DateTime.Now;
                                        _orcamentoSynapse.Update(atualizarOrcamento.OrcamentoSynapse);

                                        //Gravar o Log no Azure Table
                                        LogQualificadorLeads logQualificadorLead = new LogQualificadorLeads("LogDoQualificadorLead", Guid.NewGuid().ToString())
                                        {
                                            OrcamentoId = atualizarOrcamento.OrcamentoSynapse.InternalId,
                                            TipoOperacao = Enum.GetName(typeof(ResultadoQualificacao), atualizarOrcamento.ResultadoProcesso),
                                            LogDescricao = atualizarOrcamento.OrcamentoSynapse.LogQualificador,
                                            InicioProcesso = atualizarOrcamento.InicioProcesso,
                                            FinalProcesso = atualizarOrcamento.FinalProcesso,
                                            TempoTotalProcessamento = Convert.ToInt32((atualizarOrcamento.FinalProcesso - atualizarOrcamento.InicioProcesso).TotalMilliseconds),
                                            DataCriacao = DateTime.Now,
                                            //--------------------------------------------------------------------------------
                                            ContatoFoiCriado = atualizarOrcamento.ContatoFoiCriado,
                                            TempoExecucaoContato = Convert.ToInt32((atualizarOrcamento.FinalCriarContato - atualizarOrcamento.InicioCriarContato).TotalMilliseconds),
                                            TempoCriarLead = Convert.ToInt32((atualizarOrcamento.FinalCriarLead - atualizarOrcamento.InicioCriarLead).TotalMilliseconds),
                                            TempoCriarOportunidade = Convert.ToInt32((atualizarOrcamento.FinalCriarOpp - atualizarOrcamento.InicioCriarOpp).TotalMilliseconds),
                                            TempoAtualizarDuplicidade = Convert.ToInt32((atualizarOrcamento.FinalAtualizarDuplicidade - atualizarOrcamento.InicioAtualizarDuplicidade).TotalMilliseconds),
                                            //--------------------------------------------------------------------------------
                                        };

                                        _logQualificador.Create(logQualificadorLead);
                                    });
                                }
                                catch (Exception ex)
                                {
                                    throw (new ArgumentException(ex.Message));
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            if (ex.Message.Contains("Bearer authorization_uri"))
                            {
                                qualificadorProcesso5 = null;
                                goto ReNewConnection;
                            }
                            Console.WriteLine($"\t Erro no processo-5: {ex.Message} ");
                        }
                    }
                }
            }
        }

        public static void Processo06()
        {
            QualificadorLeads qualificadorProcesso6 = null;
            DateTime connextioCriadaEm = DateTime.Now.AddHours(-1);
            int contagem = 0;
            int contagemCpf = 0;
            int totalQualidicado = 0;
            int totalDesqualidicado = 0;
            int totalDuplicidade = 0;
            int totalAvaliar = 0;
            int totalNutrir = 0;

            foreach (var proc6 in listaCpfCnpjsProssesso06)
            {
                contagemCpf++;
                List<OrcamentoSynapse> listaOrcamentos = orcamentos.Where(o => o.PessoaCpfCnpj.Trim() == proc6.Trim()).ToList();
                if (orcamentos.Count > 0)
                {
                    foreach (var orcam in listaOrcamentos.OrderBy(o => o.NumeroItemOrcamento))
                    {
                        ReNewConnection:
                        try
                        {
                            var tempoConnetion = (DateTime.Now - connextioCriadaEm);
                            if (qualificadorProcesso6 == null)
                            {
                                connextioCriadaEm = DateTime.Now;
                                qualificadorProcesso6 = new QualificadorLeads();
                            }

                            RetornoQualificador atualizarOrcamento = qualificadorProcesso6.OrcamentoQualificarLead(orcam).GetAwaiter().GetResult();

                            //Valida se gerou erro de exception na execução do processo para o CRM, segue para o próximo registro, deixando esse registro no Cosmos para ser reprocessado.
                            if (atualizarOrcamento.OrcamentoSynapse.LogQualificador.Contains("ERRO DE EXCEÇÃO:"))
                                continue;

                            atualizarOrcamento.FinalProcesso = DateTime.Now;
                            contagem++;
                            if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Duplicidade)
                                totalDuplicidade++;
                            else if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Desqualificar)
                                totalDesqualidicado++;
                            else if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Qualificar)
                                totalQualidicado++;
                            else if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Avaliar)
                                totalAvaliar++;
                            else if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Nutrir)
                                totalNutrir++;

                            if (tempoConnetion.TotalMinutes > 30)
                            {
                                qualificadorProcesso6 = null;
                            }

                            Console.WriteLine($"  Processo 6: => Linha {contagemCpf} de {listaCpfCnpjsProssesso06.Count}, Processado: {contagem}, Qualificados: {totalQualidicado}, Desqualificados: {totalDesqualidicado}, Duplicados: {totalDuplicidade}, Avaliar: {totalAvaliar}, Nutrir: {totalNutrir} - {DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff")}");

                            if (atualizarOrcamento == null)
                            {
                                Console.WriteLine($"\t Esse orçamento deu erro no processamento ");
                            }
                            else
                            {
                                try
                                {
                                    Task.Factory.StartNew(() => {
                                        //Atualizar o Orçamento
                                        atualizarOrcamento.OrcamentoSynapse.DataModificacao = DateTime.Now;
                                        _orcamentoSynapse.Update(atualizarOrcamento.OrcamentoSynapse);

                                        //Gravar o Log no Azure Table
                                        LogQualificadorLeads logQualificadorLead = new LogQualificadorLeads("LogDoQualificadorLead", Guid.NewGuid().ToString())
                                        {
                                            OrcamentoId = atualizarOrcamento.OrcamentoSynapse.InternalId,
                                            TipoOperacao = Enum.GetName(typeof(ResultadoQualificacao), atualizarOrcamento.ResultadoProcesso),
                                            LogDescricao = atualizarOrcamento.OrcamentoSynapse.LogQualificador,
                                            InicioProcesso = atualizarOrcamento.InicioProcesso,
                                            FinalProcesso = atualizarOrcamento.FinalProcesso,
                                            TempoTotalProcessamento = Convert.ToInt32((atualizarOrcamento.FinalProcesso - atualizarOrcamento.InicioProcesso).TotalMilliseconds),
                                            DataCriacao = DateTime.Now,
                                            //--------------------------------------------------------------------------------
                                            ContatoFoiCriado = atualizarOrcamento.ContatoFoiCriado,
                                            TempoExecucaoContato = Convert.ToInt32((atualizarOrcamento.FinalCriarContato - atualizarOrcamento.InicioCriarContato).TotalMilliseconds),
                                            TempoCriarLead = Convert.ToInt32((atualizarOrcamento.FinalCriarLead - atualizarOrcamento.InicioCriarLead).TotalMilliseconds),
                                            TempoCriarOportunidade = Convert.ToInt32((atualizarOrcamento.FinalCriarOpp - atualizarOrcamento.InicioCriarOpp).TotalMilliseconds),
                                            TempoAtualizarDuplicidade = Convert.ToInt32((atualizarOrcamento.FinalAtualizarDuplicidade - atualizarOrcamento.InicioAtualizarDuplicidade).TotalMilliseconds),
                                            //--------------------------------------------------------------------------------
                                        };

                                        _logQualificador.Create(logQualificadorLead);
                                    });
                                }
                                catch (Exception ex)
                                {
                                    throw (new ArgumentException(ex.Message));
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            if (ex.Message.Contains("Bearer authorization_uri"))
                            {
                                qualificadorProcesso6 = null;
                                goto ReNewConnection;
                            }
                            Console.WriteLine($"\t Erro no processo-6: {ex.Message} ");
                        }
                    }
                }
            }
        }

        public static void Processo07()
        {
            QualificadorLeads qualificadorProcesso7 = null;
            DateTime connextioCriadaEm = DateTime.Now.AddHours(-1);
            int contagem = 0;
            int contagemCpf = 0;
            int totalQualidicado = 0;
            int totalDesqualidicado = 0;
            int totalDuplicidade = 0;
            int totalAvaliar = 0;
            int totalNutrir = 0;

            foreach (var proc7 in listaCpfCnpjsProssesso07)
            {
                contagemCpf++;
                List<OrcamentoSynapse> listaOrcamentos = orcamentos.Where(o => o.PessoaCpfCnpj.Trim() == proc7.Trim()).ToList();
                if (orcamentos.Count > 0)
                {
                    foreach (var orcam in listaOrcamentos.OrderBy(o => o.NumeroItemOrcamento))
                    {
                        ReNewConnection:
                        try
                        {
                            var tempoConnetion = (DateTime.Now - connextioCriadaEm);
                            if (qualificadorProcesso7 == null)
                            {
                                connextioCriadaEm = DateTime.Now;
                                qualificadorProcesso7 = new QualificadorLeads();
                            }

                            RetornoQualificador atualizarOrcamento = qualificadorProcesso7.OrcamentoQualificarLead(orcam).GetAwaiter().GetResult();

                            //Valida se gerou erro de exception na execução do processo para o CRM, segue para o próximo registro, deixando esse registro no Cosmos para ser reprocessado.
                            if (atualizarOrcamento.OrcamentoSynapse.LogQualificador.Contains("ERRO DE EXCEÇÃO:"))
                                continue;

                            atualizarOrcamento.FinalProcesso = DateTime.Now;
                            contagem++;
                            if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Duplicidade)
                                totalDuplicidade++;
                            else if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Desqualificar)
                                totalDesqualidicado++;
                            else if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Qualificar)
                                totalQualidicado++;
                            else if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Avaliar)
                                totalAvaliar++;
                            else if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Nutrir)
                                totalNutrir++;

                            if (tempoConnetion.TotalMinutes > 30)
                            {
                                qualificadorProcesso7 = null;
                            }

                            Console.WriteLine($"  Processo 7: => Linha {contagemCpf} de {listaCpfCnpjsProssesso07.Count}, Processado: {contagem}, Qualificados: {totalQualidicado}, Desqualificados: {totalDesqualidicado}, Duplicados: {totalDuplicidade}, Avaliar: {totalAvaliar}, Nutrir: {totalNutrir} - {DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff")}");

                            if (atualizarOrcamento == null)
                            {
                                Console.WriteLine($"\t Esse orçamento deu erro no processamento ");
                            }
                            else
                            {
                                try
                                {
                                    Task.Factory.StartNew(() => {
                                        //Atualizar o Orçamento
                                        atualizarOrcamento.OrcamentoSynapse.DataModificacao = DateTime.Now;
                                        _orcamentoSynapse.Update(atualizarOrcamento.OrcamentoSynapse);

                                        //Gravar o Log no Azure Table
                                        LogQualificadorLeads logQualificadorLead = new LogQualificadorLeads("LogDoQualificadorLead", Guid.NewGuid().ToString())
                                        {
                                            OrcamentoId = atualizarOrcamento.OrcamentoSynapse.InternalId,
                                            TipoOperacao = Enum.GetName(typeof(ResultadoQualificacao), atualizarOrcamento.ResultadoProcesso),
                                            LogDescricao = atualizarOrcamento.OrcamentoSynapse.LogQualificador,
                                            InicioProcesso = atualizarOrcamento.InicioProcesso,
                                            FinalProcesso = atualizarOrcamento.FinalProcesso,
                                            TempoTotalProcessamento = Convert.ToInt32((atualizarOrcamento.FinalProcesso - atualizarOrcamento.InicioProcesso).TotalMilliseconds),
                                            DataCriacao = DateTime.Now,
                                            //--------------------------------------------------------------------------------
                                            ContatoFoiCriado = atualizarOrcamento.ContatoFoiCriado,
                                            TempoExecucaoContato = Convert.ToInt32((atualizarOrcamento.FinalCriarContato - atualizarOrcamento.InicioCriarContato).TotalMilliseconds),
                                            TempoCriarLead = Convert.ToInt32((atualizarOrcamento.FinalCriarLead - atualizarOrcamento.InicioCriarLead).TotalMilliseconds),
                                            TempoCriarOportunidade = Convert.ToInt32((atualizarOrcamento.FinalCriarOpp - atualizarOrcamento.InicioCriarOpp).TotalMilliseconds),
                                            TempoAtualizarDuplicidade = Convert.ToInt32((atualizarOrcamento.FinalAtualizarDuplicidade - atualizarOrcamento.InicioAtualizarDuplicidade).TotalMilliseconds),
                                            //--------------------------------------------------------------------------------
                                        };

                                        _logQualificador.Create(logQualificadorLead);
                                    });
                                }
                                catch (Exception ex)
                                {
                                    throw (new ArgumentException(ex.Message));
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            if (ex.Message.Contains("Bearer authorization_uri"))
                            {
                                qualificadorProcesso7 = null;
                                goto ReNewConnection;
                            }
                            Console.WriteLine($"\t Erro no processo-7: {ex.Message} ");
                        }
                    }
                }
            }
        }

        public static void Processo08()
        {
            QualificadorLeads qualificadorProcesso8 = null;
            DateTime connextioCriadaEm = DateTime.Now.AddHours(-1);
            int contagem = 0;
            int contagemCpf = 0;
            int totalQualidicado = 0;
            int totalDesqualidicado = 0;
            int totalDuplicidade = 0;
            int totalAvaliar = 0;
            int totalNutrir = 0;

            foreach (var proc8 in listaCpfCnpjsProssesso08)
            {
                contagemCpf++;
                List<OrcamentoSynapse> listaOrcamentos = orcamentos.Where(o => o.PessoaCpfCnpj.Trim() == proc8.Trim()).ToList();
                if (orcamentos.Count > 0)
                {
                    foreach (var orcam in listaOrcamentos.OrderBy(o => o.NumeroItemOrcamento))
                    {
                        ReNewConnection:
                        try
                        {
                            var tempoConnetion = (DateTime.Now - connextioCriadaEm);
                            if (qualificadorProcesso8 == null)
                            {
                                connextioCriadaEm = DateTime.Now;
                                qualificadorProcesso8 = new QualificadorLeads();
                            }

                            RetornoQualificador atualizarOrcamento = qualificadorProcesso8.OrcamentoQualificarLead(orcam).GetAwaiter().GetResult();

                            //Valida se gerou erro de exception na execução do processo para o CRM, segue para o próximo registro, deixando esse registro no Cosmos para ser reprocessado.
                            if (atualizarOrcamento.OrcamentoSynapse.LogQualificador.Contains("ERRO DE EXCEÇÃO:"))
                                continue;

                            atualizarOrcamento.FinalProcesso = DateTime.Now;
                            contagem++;
                            if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Duplicidade)
                                totalDuplicidade++;
                            else if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Desqualificar)
                                totalDesqualidicado++;
                            else if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Qualificar)
                                totalQualidicado++;
                            else if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Avaliar)
                                totalAvaliar++;
                            else if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Nutrir)
                                totalNutrir++;

                            if (tempoConnetion.TotalMinutes > 30)
                            {
                                qualificadorProcesso8 = null;
                            }

                            Console.WriteLine($"  Processo 8: => Linha {contagemCpf} de {listaCpfCnpjsProssesso08.Count}, Processado: {contagem}, Qualificados: {totalQualidicado}, Desqualificados: {totalDesqualidicado}, Duplicados: {totalDuplicidade}, Avaliar: {totalAvaliar}, Nutrir: {totalNutrir} - {DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff")}");

                            if (atualizarOrcamento == null)
                            {
                                Console.WriteLine($"\t Esse orçamento deu erro no processamento ");
                            }
                            else
                            {
                                try
                                {
                                    Task.Factory.StartNew(() => {
                                        //Atualizar o Orçamento
                                        atualizarOrcamento.OrcamentoSynapse.DataModificacao = DateTime.Now;
                                        _orcamentoSynapse.Update(atualizarOrcamento.OrcamentoSynapse);

                                        //Gravar o Log no Azure Table
                                        LogQualificadorLeads logQualificadorLead = new LogQualificadorLeads("LogDoQualificadorLead", Guid.NewGuid().ToString())
                                        {
                                            OrcamentoId = atualizarOrcamento.OrcamentoSynapse.InternalId,
                                            TipoOperacao = Enum.GetName(typeof(ResultadoQualificacao), atualizarOrcamento.ResultadoProcesso),
                                            LogDescricao = atualizarOrcamento.OrcamentoSynapse.LogQualificador,
                                            InicioProcesso = atualizarOrcamento.InicioProcesso,
                                            FinalProcesso = atualizarOrcamento.FinalProcesso,
                                            TempoTotalProcessamento = Convert.ToInt32((atualizarOrcamento.FinalProcesso - atualizarOrcamento.InicioProcesso).TotalMilliseconds),
                                            DataCriacao = DateTime.Now,
                                            //--------------------------------------------------------------------------------
                                            ContatoFoiCriado = atualizarOrcamento.ContatoFoiCriado,
                                            TempoExecucaoContato = Convert.ToInt32((atualizarOrcamento.FinalCriarContato - atualizarOrcamento.InicioCriarContato).TotalMilliseconds),
                                            TempoCriarLead = Convert.ToInt32((atualizarOrcamento.FinalCriarLead - atualizarOrcamento.InicioCriarLead).TotalMilliseconds),
                                            TempoCriarOportunidade = Convert.ToInt32((atualizarOrcamento.FinalCriarOpp - atualizarOrcamento.InicioCriarOpp).TotalMilliseconds),
                                            TempoAtualizarDuplicidade = Convert.ToInt32((atualizarOrcamento.FinalAtualizarDuplicidade - atualizarOrcamento.InicioAtualizarDuplicidade).TotalMilliseconds),
                                            //--------------------------------------------------------------------------------
                                        };

                                        _logQualificador.Create(logQualificadorLead);
                                    });
                                }
                                catch (Exception ex)
                                {
                                    throw (new ArgumentException(ex.Message));
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            if (ex.Message.Contains("Bearer authorization_uri"))
                            {
                                qualificadorProcesso8 = null;
                                goto ReNewConnection;
                            }
                            Console.WriteLine($"\t Erro no processo-8: {ex.Message} ");
                        }
                    }
                }
            }
        }

        public static void Processo09()
        {
            QualificadorLeads qualificadorProcesso9 = null;
            DateTime connextioCriadaEm = DateTime.Now.AddHours(-1);
            int contagem = 0;
            int contagemCpf = 0;
            int totalQualidicado = 0;
            int totalDesqualidicado = 0;
            int totalDuplicidade = 0;
            int totalAvaliar = 0;
            int totalNutrir = 0;

            foreach (var proc9 in listaCpfCnpjsProssesso09)
            {
                contagemCpf++;
                List<OrcamentoSynapse> listaOrcamentos = orcamentos.Where(o => o.PessoaCpfCnpj.Trim() == proc9.Trim()).ToList();
                if (orcamentos.Count > 0)
                {
                    foreach (var orcam in listaOrcamentos.OrderBy(o => o.NumeroItemOrcamento))
                    {
                        ReNewConnection:
                        try
                        {
                            var tempoConnetion = (DateTime.Now - connextioCriadaEm);
                            if (qualificadorProcesso9 == null)
                            {
                                connextioCriadaEm = DateTime.Now;
                                qualificadorProcesso9 = new QualificadorLeads();
                            }

                            RetornoQualificador atualizarOrcamento = qualificadorProcesso9.OrcamentoQualificarLead(orcam).GetAwaiter().GetResult();

                            //Valida se gerou erro de exception na execução do processo para o CRM, segue para o próximo registro, deixando esse registro no Cosmos para ser reprocessado.
                            if (atualizarOrcamento.OrcamentoSynapse.LogQualificador.Contains("ERRO DE EXCEÇÃO:"))
                                continue;

                            atualizarOrcamento.FinalProcesso = DateTime.Now;
                            contagem++;
                            if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Duplicidade)
                                totalDuplicidade++;
                            else if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Desqualificar)
                                totalDesqualidicado++;
                            else if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Qualificar)
                                totalQualidicado++;
                            else if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Avaliar)
                                totalAvaliar++;
                            else if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Nutrir)
                                totalNutrir++;

                            if (tempoConnetion.TotalMinutes > 30)
                            {
                                qualificadorProcesso9 = null;
                            }

                            Console.WriteLine($"  Processo 9: => Linha {contagemCpf} de {listaCpfCnpjsProssesso09.Count}, Processado: {contagem}, Qualificados: {totalQualidicado}, Desqualificados: {totalDesqualidicado}, Duplicados: {totalDuplicidade}, Avaliar: {totalAvaliar}, Nutrir: {totalNutrir} - {DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff")}");

                            if (atualizarOrcamento == null)
                            {
                                Console.WriteLine($"\t Esse orçamento deu erro no processamento ");
                            }
                            else
                            {
                                try
                                {
                                    Task.Factory.StartNew(() => {
                                        //Atualizar o Orçamento
                                        atualizarOrcamento.OrcamentoSynapse.DataModificacao = DateTime.Now;
                                        _orcamentoSynapse.Update(atualizarOrcamento.OrcamentoSynapse);

                                        //Gravar o Log no Azure Table
                                        LogQualificadorLeads logQualificadorLead = new LogQualificadorLeads("LogDoQualificadorLead", Guid.NewGuid().ToString())
                                        {
                                            OrcamentoId = atualizarOrcamento.OrcamentoSynapse.InternalId,
                                            TipoOperacao = Enum.GetName(typeof(ResultadoQualificacao), atualizarOrcamento.ResultadoProcesso),
                                            LogDescricao = atualizarOrcamento.OrcamentoSynapse.LogQualificador,
                                            InicioProcesso = atualizarOrcamento.InicioProcesso,
                                            FinalProcesso = atualizarOrcamento.FinalProcesso,
                                            TempoTotalProcessamento = Convert.ToInt32((atualizarOrcamento.FinalProcesso - atualizarOrcamento.InicioProcesso).TotalMilliseconds),
                                            DataCriacao = DateTime.Now,
                                            //--------------------------------------------------------------------------------
                                            ContatoFoiCriado = atualizarOrcamento.ContatoFoiCriado,
                                            TempoExecucaoContato = Convert.ToInt32((atualizarOrcamento.FinalCriarContato - atualizarOrcamento.InicioCriarContato).TotalMilliseconds),
                                            TempoCriarLead = Convert.ToInt32((atualizarOrcamento.FinalCriarLead - atualizarOrcamento.InicioCriarLead).TotalMilliseconds),
                                            TempoCriarOportunidade = Convert.ToInt32((atualizarOrcamento.FinalCriarOpp - atualizarOrcamento.InicioCriarOpp).TotalMilliseconds),
                                            TempoAtualizarDuplicidade = Convert.ToInt32((atualizarOrcamento.FinalAtualizarDuplicidade - atualizarOrcamento.InicioAtualizarDuplicidade).TotalMilliseconds),
                                            //--------------------------------------------------------------------------------
                                        };

                                        _logQualificador.Create(logQualificadorLead);
                                    });
                                }
                                catch (Exception ex)
                                {
                                    throw (new ArgumentException(ex.Message));
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            if (ex.Message.Contains("Bearer authorization_uri"))
                            {
                                qualificadorProcesso9 = null;
                                goto ReNewConnection;
                            }
                            Console.WriteLine($"\t Erro no processo-9: {ex.Message} ");
                        }
                    }
                }
            }
        }

        public static void Processo10()
        {
            QualificadorLeads qualificadorProcesso10 = null;
            DateTime connextioCriadaEm = DateTime.Now.AddHours(-1);
            int contagem = 0;
            int contagemCpf = 0;
            int totalQualidicado = 0;
            int totalDesqualidicado = 0;
            int totalDuplicidade = 0;
            int totalAvaliar = 0;
            int totalNutrir = 0;

            foreach (var proc10 in listaCpfCnpjsProssesso10)
            {
                contagemCpf++;
                List<OrcamentoSynapse> listaOrcamentos = orcamentos.Where(o => o.PessoaCpfCnpj.Trim() == proc10.Trim()).ToList();
                if (orcamentos.Count > 0)
                {
                    foreach (var orcam in listaOrcamentos.OrderBy(o => o.NumeroItemOrcamento))
                    {
                        ReNewConnection:
                        try
                        {
                            var tempoConnetion = (DateTime.Now - connextioCriadaEm);
                            if (qualificadorProcesso10 == null)
                            {
                                connextioCriadaEm = DateTime.Now;
                                qualificadorProcesso10 = new QualificadorLeads();
                            }

                            RetornoQualificador atualizarOrcamento = qualificadorProcesso10.OrcamentoQualificarLead(orcam).GetAwaiter().GetResult();

                            //Valida se gerou erro de exception na execução do processo para o CRM, segue para o próximo registro, deixando esse registro no Cosmos para ser reprocessado.
                            if (atualizarOrcamento.OrcamentoSynapse.LogQualificador.Contains("ERRO DE EXCEÇÃO:"))
                                continue;

                            atualizarOrcamento.FinalProcesso = DateTime.Now;
                            contagem++;
                            if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Duplicidade)
                                totalDuplicidade++;
                            else if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Desqualificar)
                                totalDesqualidicado++;
                            else if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Qualificar)
                                totalQualidicado++;
                            else if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Avaliar)
                                totalAvaliar++;
                            else if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Nutrir)
                                totalNutrir++;

                            if (tempoConnetion.TotalMinutes > 30)
                            {
                                qualificadorProcesso10 = null;
                            }

                            Console.WriteLine($"  Processo 10: => Linha {contagemCpf} de {listaCpfCnpjsProssesso10.Count}, Processado: {contagem}, Qualificados: {totalQualidicado}, Desqualificados: {totalDesqualidicado}, Duplicados: {totalDuplicidade}, Avaliar: {totalAvaliar}, Nutrir: {totalNutrir} - {DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff")}");

                            if (atualizarOrcamento == null)
                            {
                                Console.WriteLine($"\t Esse orçamento deu erro no processamento ");
                            }
                            else
                            {
                                try
                                {
                                    Task.Factory.StartNew(() => {
                                        //Atualizar o Orçamento
                                        atualizarOrcamento.OrcamentoSynapse.DataModificacao = DateTime.Now;
                                        _orcamentoSynapse.Update(atualizarOrcamento.OrcamentoSynapse);

                                        //Gravar o Log no Azure Table
                                        LogQualificadorLeads logQualificadorLead = new LogQualificadorLeads("LogDoQualificadorLead", Guid.NewGuid().ToString())
                                        {
                                            OrcamentoId = atualizarOrcamento.OrcamentoSynapse.InternalId,
                                            TipoOperacao = Enum.GetName(typeof(ResultadoQualificacao), atualizarOrcamento.ResultadoProcesso),
                                            LogDescricao = atualizarOrcamento.OrcamentoSynapse.LogQualificador,
                                            InicioProcesso = atualizarOrcamento.InicioProcesso,
                                            FinalProcesso = atualizarOrcamento.FinalProcesso,
                                            TempoTotalProcessamento = Convert.ToInt32((atualizarOrcamento.FinalProcesso - atualizarOrcamento.InicioProcesso).TotalMilliseconds),
                                            DataCriacao = DateTime.Now,
                                            //--------------------------------------------------------------------------------
                                            ContatoFoiCriado = atualizarOrcamento.ContatoFoiCriado,
                                            TempoExecucaoContato = Convert.ToInt32((atualizarOrcamento.FinalCriarContato - atualizarOrcamento.InicioCriarContato).TotalMilliseconds),
                                            TempoCriarLead = Convert.ToInt32((atualizarOrcamento.FinalCriarLead - atualizarOrcamento.InicioCriarLead).TotalMilliseconds),
                                            TempoCriarOportunidade = Convert.ToInt32((atualizarOrcamento.FinalCriarOpp - atualizarOrcamento.InicioCriarOpp).TotalMilliseconds),
                                            TempoAtualizarDuplicidade = Convert.ToInt32((atualizarOrcamento.FinalAtualizarDuplicidade - atualizarOrcamento.InicioAtualizarDuplicidade).TotalMilliseconds),
                                            //--------------------------------------------------------------------------------
                                        };

                                        _logQualificador.Create(logQualificadorLead);
                                    });
                                }
                                catch (Exception ex)
                                {
                                    throw (new ArgumentException(ex.Message));
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            if (ex.Message.Contains("Bearer authorization_uri"))
                            {
                                qualificadorProcesso10 = null;
                                goto ReNewConnection;
                            }
                            Console.WriteLine($"\t Erro no processo-10: {ex.Message} ");
                        }
                    }
                }
            }
        }
        #endregion

        public static void ProcessarThreads(List<string> listaCpfCnpjs, string nomethread)
        {
            //RepositoryService repositoryService = new RepositoryService(_orgDynamics, false, _provider);
            QualificadorLeads qualificador = null;
            DateTime connextioCriadaEm = DateTime.Now;
            int contagem = 0;
            int contagemCpf = 0;
            int totalQualidicado = 0;
            int totalDesqualidicado = 0;
            int totalDuplicidade = 0;
            int totalAvaliar = 0;
            int totalNutrir = 0;

            //ExecuteMultipleRequest executeMultiplo = new ExecuteMultipleRequest()
            //{
            //    Settings = new ExecuteMultipleSettings()
            //    {
            //        ContinueOnError = true,
            //        ReturnResponses = true
            //    },
            //    Requests = new OrganizationRequestCollection()
            //};

            foreach (var proc1 in listaCpfCnpjs)
            {
                contagemCpf++;
                List<OrcamentoSynapse> listaOrcamentos = orcamentos.Where(o => o.PessoaCpfCnpj.Trim() == proc1.Trim()).ToList();
                if (orcamentos.Count > 0)
                {
                    foreach (var orcam in listaOrcamentos.OrderBy(o => o.NumeroItemOrcamento))
                    {
                    ReNewConnection:
                        try
                        {
                            var tempoConnetion = (DateTime.Now - connextioCriadaEm);
                            if (qualificador == null)
                            {
                                connextioCriadaEm = DateTime.Now;
                                qualificador = new QualificadorLeads();
                            }

                            RetornoQualificador atualizarOrcamento = qualificador.OrcamentoQualificarLead(orcam).GetAwaiter().GetResult();

                            //Valida se gerou erro de exception na execução do processo para o CRM, segue para o próximo registro, deixando esse registro no Cosmos para ser reprocessado.
                            if (atualizarOrcamento.OrcamentoSynapse.LogQualificador.Contains("ERRO DE EXCEÇÃO:"))
                                continue;

                            atualizarOrcamento.FinalProcesso = DateTime.Now;
                            contagem++;
                            if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Duplicidade)
                                totalDuplicidade++;
                            else if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Desqualificar)
                                totalDesqualidicado++;
                            else if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Qualificar)
                                totalQualidicado++;
                            else if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Avaliar)
                                totalAvaliar++;
                            else if (atualizarOrcamento.ResultadoProcesso == (Int32)ResultadoQualificacao.Nutrir)
                                totalNutrir++;

                            //if (tempoConnetion.TotalMinutes > 30)
                            //{
                            //    qualificadorProcesso1 = null;
                            //}
                            Console.WriteLine($"  Processo: {nomethread} => Linha {contagemCpf} de {listaCpfCnpjs.Count}, Processado: {contagem}, Qualificados: {totalQualidicado}, Desqualificados: {totalDesqualidicado}, Duplicados: {totalDuplicidade}, Avaliar: {totalAvaliar}, Nutrir: {totalNutrir} - {DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff")}");

                            #region Adiciona lista para execução multipla
                            ////--------------------------EXECUSSÃO MULTIPLA--------------------------\\
                            //if (atualizarOrcamento.OportunidadeCreate != null)
                            //{
                            //    CreateRequest createRequest = new CreateRequest { Target = atualizarOrcamento.OportunidadeCreate };
                            //    executeMultiplo.Requests.Add(createRequest);
                            //}
                            //else if (atualizarOrcamento.LeadCreate != null)
                            //{
                            //    CreateRequest createRequest = new CreateRequest { Target = atualizarOrcamento.LeadCreate };
                            //    executeMultiplo.Requests.Add(createRequest);
                            //}
                            //else if (atualizarOrcamento.OportunidadeUpdate != null)
                            //{
                            //    UpdateRequest updateRequest = new UpdateRequest { Target = atualizarOrcamento.OportunidadeUpdate };
                            //    executeMultiplo.Requests.Add(updateRequest);
                            //}
                            //else if (atualizarOrcamento.LeadUpdate != null)
                            //{
                            //    UpdateRequest updateRequest = new UpdateRequest { Target = atualizarOrcamento.LeadUpdate };
                            //    executeMultiplo.Requests.Add(updateRequest);
                            //}
                            ////----------------------------------------------------------------------\\
                            #endregion

                            if (atualizarOrcamento == null)
                            {
                                Console.WriteLine($"\t Esse orçamento deu erro no processamento ");
                            }
                            else
                            {
                                try
                                {
                                    Task.Factory.StartNew(() => {
                                        //Atualizar o Orçamento
                                        atualizarOrcamento.OrcamentoSynapse.DataModificacao = DateTime.Now;
                                        _orcamentoSynapse.Update(atualizarOrcamento.OrcamentoSynapse);

                                        //Gravar o Log no Azure Table
                                        LogQualificadorLeads logQualificadorLead = new LogQualificadorLeads("LogDoQualificadorLead", Guid.NewGuid().ToString())
                                        {
                                            OrcamentoId = atualizarOrcamento.OrcamentoSynapse.InternalId,
                                            TipoOperacao = Enum.GetName(typeof(ResultadoQualificacao), atualizarOrcamento.ResultadoProcesso),
                                            LogDescricao = atualizarOrcamento.OrcamentoSynapse.LogQualificador,
                                            InicioProcesso = atualizarOrcamento.InicioProcesso,
                                            FinalProcesso = atualizarOrcamento.FinalProcesso,
                                            TempoTotalProcessamento = Convert.ToInt32((atualizarOrcamento.FinalProcesso - atualizarOrcamento.InicioProcesso).TotalMilliseconds),
                                            DataCriacao = DateTime.Now,
                                            //--------------------------------------------------------------------------------
                                            ContatoFoiCriado = atualizarOrcamento.ContatoFoiCriado,
                                            TempoExecucaoContato = Convert.ToInt32((atualizarOrcamento.FinalCriarContato - atualizarOrcamento.InicioCriarContato).TotalMilliseconds),
                                            TempoCriarLead = Convert.ToInt32((atualizarOrcamento.FinalCriarLead - atualizarOrcamento.InicioCriarLead).TotalMilliseconds),
                                            TempoCriarOportunidade = Convert.ToInt32((atualizarOrcamento.FinalCriarOpp - atualizarOrcamento.InicioCriarOpp).TotalMilliseconds),
                                            TempoAtualizarDuplicidade = Convert.ToInt32((atualizarOrcamento.FinalAtualizarDuplicidade - atualizarOrcamento.InicioAtualizarDuplicidade).TotalMilliseconds),
                                            //--------------------------------------------------------------------------------
                                        };

                                        _logQualificador.Create(logQualificadorLead);
                                    });
                                }
                                catch (Exception ex)
                                {
                                    throw (new ArgumentException(ex.Message));
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            if (ex.Message.Contains("Bearer authorization_uri"))
                            {
                                qualificador = null;
                                goto ReNewConnection;
                            }
                            Console.WriteLine($"\t Erro no processo-{nomethread}: {ex.Message} ");
                        }
                    }
                }
                #region Executa blocos de 20
                ////--------------------------EXECUSSÃO MULTIPLA--------------------------\\
                //if (executeMultiplo.Requests.Count >= 20)
                //{
                //    ExecuteMultipleResponse responseWithResults = (ExecuteMultipleResponse)((IOrganizationService)repositoryService.CrmServiceProvider.Provider).Execute(executeMultiplo);

                //    executeMultiplo = new ExecuteMultipleRequest()
                //    {
                //        Settings = new ExecuteMultipleSettings()
                //        {
                //            ContinueOnError = true,
                //            ReturnResponses = true
                //        },
                //        Requests = new OrganizationRequestCollection()
                //    };
                //}
                ////----------------------------------------------------------------------\\
                #endregion
            }
            #region Executa o restante
            ////--------------------------EXECUSSÃO MULTIPLA--------------------------\\
            //if (executeMultiplo.Requests.Count > 0)
            //{
            //    ExecuteMultipleResponse responseWithResults = (ExecuteMultipleResponse)((IOrganizationService)repositoryService.CrmServiceProvider.Provider).Execute(executeMultiplo);
            //}
            ////----------------------------------------------------------------------\\
            #endregion
        }
    }

    public class ScoreSerasaCache
    {
        public string CPFCNPJ { get; set; }
        public bool? TemRestricao { get; set; }
        public int ScoreCSBA { get; set; }
        public int configuracaoLimiteScoreCSBA { get; set; }
    }
}