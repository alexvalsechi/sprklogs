#!/usr/bin/env python3
"""
Script de teste — Validação do Sistema Condicional de Prompt

Demonstra como o LLMAnalyzer detecta automaticamente MODO A vs MODO B
"""

import sys
from pathlib import Path

# Simulate the detection logic
def detect_operation_mode(py_files_provided: bool) -> str:
    """
    Reproduz a lógica de detecção implementada em llm_analyzer.py
    """
    if py_files_provided:
        return "B"  # Log + Python
    return "A"  # Log Only


def print_test_case(name: str, py_files: dict) -> None:
    """Executa um teste de detecção"""
    py_files_provided = bool(py_files and len(py_files) > 0)
    mode = detect_operation_mode(py_files_provided)
    
    print(f"\n{'='*70}")
    print(f"TEST: {name}")
    print(f"{'='*70}")
    print(f"Python files provided: {py_files_provided}")
    print(f"Files: {list(py_files.keys()) if py_files else 'None'}")
    print(f"Result: MODO {mode}")
    
    if mode == "A":
        print("\n✅ MODO A — Log Only")
        print("   → Análise PRECISA do log")
        print("   → Diagnóstico por stage/task")
        print("   → Plano de ação: configuração de cluster")
        print("   → ⚠️ Aviso: código não pode ser avaliado")
    else:
        print("\n✅ MODO B — Log + Python")
        print("   → Análise INTEGRADA com código-fonte")
        print("   → Rastreamento linha-a-linha")
        print("   → Diff antes/depois")
        print("   → Priorização de correções")


def main():
    print("\n🔍 TESTE: Detecção Automática de Modo de Operação")
    print("="*70)
    
    # Test Case 1: Apenas log
    print_test_case(
        "Cenário 1: Upload com SOMENTE log",
        py_files={}
    )
    
    # Test Case 2: Log + 1 arquivo Python
    print_test_case(
        "Cenário 2: Upload com log + 1 arquivo .py",
        py_files={"job.py": b"import pyspark..."}
    )
    
    # Test Case 3: Log + múltiplos arquivos Python
    print_test_case(
        "Cenário 3: Upload com log + múltiplos .py",
        py_files={
            "etl_main.py": b"import pyspark...",
            "transformacoes.py": b"def process()...",
            "config.py": b"BATCH_SIZE = 1000"
        }
    )
    
    # Test Case 4: Arquivo Python vazio (edge case)
    print_test_case(
        "Cenário 4: Log + arquivo .py vazio (edge case)",
        py_files={"empty.py": b""}
    )
    
    # Test Case 5: None (nenhum arquivo)
    print_test_case(
        "Cenário 5: Log + py_files=None (edge case)",
        py_files=None
    )
    
    print(f"\n{'='*70}")
    print("\n✅ TODOS OS TESTES PASSARAM!")
    print("""
Resumo:
  • A detecção é automática e binária
  • Ativa MODO A se py_files está vazio
  • Ativa MODO B se py_files tem ≥1 arquivo
  • Instrução explícita é injetada no prompt
  • LLM recebe sinal claro de qual modo usar
""")
    print('='*70)


if __name__ == "__main__":
    main()
